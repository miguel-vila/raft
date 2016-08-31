package raft

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.Timeout
import org.scalatest._
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import akka.pattern._
import scala.util.{ Failure, Success }
import org.scalatest.concurrent.ScalaFutures

class NodeActorSpec extends FlatSpec with Matchers with ScalaFutures {

  def initialStateMachine(): IntNumberState = new IntNumberState()

  def verifySingleLeaderAndFollowers(state: Map[NodeId, NodeState]): Boolean = {
    val leader = state.find {
      case (_, nodeState) =>
        nodeState match {
          case Leader(_, _) => true
          case _ => false
        }
    }
    leader match {
      case Some(leader) =>
        val others = state.filter(_ != leader)
        val (leaderId, Leader(_, leaderTerm)) = leader
        others.forall {
          case (_, nodeState) =>
            nodeState match {
              case Follower(leaderIdOpt, followerTerm) =>
                leaderIdOpt == Some(leaderId) && followerTerm == leaderTerm
              case _ => false
            }
        }
      case None =>
        false
    }
  }

  def getNodesState(nodes: Map[NodeId, ActorRef])(implicit executionContext: ExecutionContext, timeout: Timeout): Future[Map[NodeId, NodeState]] = {
    Future.traverse(nodes) {
      case (nodeId, node) =>
        (node ? QueryState).mapTo[NodeState]
          .map(state => nodeId -> state)
    }.map(_.toMap)
  }

  "NodeActor" should "elect as a leader the first node that starts an election" in {
    implicit val actorSystem: ActorSystem = ActorSystem("test-system-1")
    val allNodes: Map[NodeId, ActorRef] = {
      val node0 = NodeActor.createActor(0, initialStateMachine(), 150.millis, 100.millis)
      val node1 = NodeActor.createActor(1, initialStateMachine(), 275.millis, 100.millis)
      val node2 = NodeActor.createActor(2, initialStateMachine(), 275.millis, 100.millis)
      val node3 = NodeActor.createActor(3, initialStateMachine(), 275.millis, 100.millis)
      val node4 = NodeActor.createActor(4, initialStateMachine(), 300.millis, 100.millis)
      Map(
        0 -> node0,
        1 -> node1,
        2 -> node2,
        3 -> node3,
        4 -> node4
      )
    }
    allNodes.foreach {
      case (id, node) =>
        node ! SetNodesInfo(allNodes.filter { case (nodeId, _) => nodeId != id })
    }
    Thread.sleep(750)
    implicit val ec = actorSystem.dispatcher
    implicit val timeout = Timeout(100.millis)

    getNodesState(allNodes).onComplete {
      case Success(states) =>
        actorSystem.shutdown()
        verifySingleLeaderAndFollowers(states) should equal(true)
      case Failure(err) =>
        err.printStackTrace()
        actorSystem.shutdown()
        fail()
    }
  }
}
