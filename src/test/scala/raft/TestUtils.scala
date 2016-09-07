package raft

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.util.Timeout
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import akka.pattern._

trait TestUtils {

  def getLeadersRefs(nodes: Nodes, state: ClusterState): List[(NodeId, ActorRef)] = {
    state.filter {
      case (_, Leader(_, _)) => true
      case _ => false
    }.toList.map {
      case (nodeId, _) =>
        nodeId -> nodes(nodeId)
    }
  }

  def initialStateMachine(): IntNumberState = new IntNumberState()

  def startNodes[A](nodes: Nodes, network: Network[A]): Unit =
    nodes.foreach {
      case (id, node) =>
        node ! SetNodesInfo(
          nodes.keys.filter { case nodeId => nodeId != id }.toList,
          network
        )
    }

  def setupSimpleNodeSystem()(implicit actorSystem: ActorSystem): Nodes = {
    val requestTimeout = 150.millis
    val node0 = NodeActor.createActor(0, initialStateMachine(), 150.millis, 100.millis, requestTimeout, s"node0")
    val node1 = NodeActor.createActor(1, initialStateMachine(), 225.millis, 100.millis, requestTimeout, s"node1")
    val node2 = NodeActor.createActor(2, initialStateMachine(), 275.millis, 100.millis, requestTimeout, s"node2")
    val node3 = NodeActor.createActor(3, initialStateMachine(), 250.millis, 100.millis, requestTimeout, s"node3")
    val node4 = NodeActor.createActor(4, initialStateMachine(), 300.millis, 100.millis, requestTimeout, s"node4")
    val nodes = Map(
      0 -> node0,
      1 -> node1,
      2 -> node2,
      3 -> node3,
      4 -> node4
    )
    val network = new ActorNetwork[Int](nodes)
    startNodes(nodes, network)
    nodes
  }

  def getNodesState(nodes: Nodes)(implicit executionContext: ExecutionContext, timeout: Timeout): Future[ClusterState] = {
    Future.traverse(nodes) {
      case (nodeId, node) =>
        (node ? QueryState).mapTo[NodeState]
          .recover {
            case t: Throwable =>
              FailedNode(-1)
          }.map { state =>
          nodeId -> (state match {
            case active: ActiveState => active
            case _ => throw new Exception("All nodes should be active")
          })
        }
    }.map(_.toMap)
  }

  def makeNodeFail(node: ActorRef)(implicit actorSystem: ActorSystem): Unit = {
    actorSystem.stop(node)
  }

  def makeLeadersFail(nodes: Nodes, state: ClusterState, verifyNumberOfLeaders: Option[Int])(implicit actorSysten: ActorSystem): Unit = {
    val leadersRefs = getLeadersRefs(nodes, state)
    verifyNumberOfLeaders foreach { expectedLeaders => assert(leadersRefs.size == expectedLeaders) }
    leadersRefs foreach {
      case (nodeId, nodeRef) =>
        println(s"Making node $nodeId fail")
        makeNodeFail(nodeRef)
    }
  }

}
