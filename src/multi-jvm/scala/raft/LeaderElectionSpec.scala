package raft

import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeSpec
import akka.pattern._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{ Millis, Seconds, Span }
import scala.concurrent.Future

class LeaderElectionSpecMultiJvmNode1 extends LeaderElectionSpec
class LeaderElectionSpecMultiJvmNode2 extends LeaderElectionSpec
class LeaderElectionSpecMultiJvmNode3 extends LeaderElectionSpec
class LeaderElectionSpecMultiJvmNode4 extends LeaderElectionSpec
class LeaderElectionSpecMultiJvmNode5 extends LeaderElectionSpec

class LeaderElectionSpec extends MultiNodeSpec(SampleMultiNodeConfig)
    with STMultiNodeSpec
    with TestVerifications
    with ScalaFutures {

  import SampleMultiNodeConfig._

  def initialParticipants = nodes.size

  implicit val patience = PatienceConfig(timeout = Span(1, Seconds), interval = Span(5, Millis))

  "On leader election" must {

    "wait for all nodes to enter a barrier" in {
      enterBarrier("startup")
          
    }

    "elect a leader on startup" in {

      def pathFor(role: RoleName) = node(role)

      val allNodes = nodes
        .map { case (otherNodeId,otherNode) =>
          otherNodeId -> (system.actorSelection(pathFor(otherNode) / "user" / name(otherNodeId)))
      }.toMap

      nodes foreach { case (nodeId,node) =>
        runOn(node) {
          enterBarrier("start")
          val nodeActor = createActorRef(nodeId)

          val otherNodes = allNodes.filter { case (otherNodeId,_) =>
              nodeId != otherNodeId
          }
          enterBarrier(s"actor-ref-created")

          nodeActor ! SetNodesInfo(otherNodes)

          enterBarrier(s"running")

          Thread.sleep(750)

          enterBarrier(s"waited-election")

          import system.dispatcher

          val nodesState = Future.traverse(allNodes.toList) { case (nodeId, nodeSelection) =>
            (nodeSelection ? QueryState)(askTimeout)
              .recover { case t: Throwable =>
                FailedNode(-1)
            }.mapTo[ActiveState]
              .map( state => nodeId -> state )
          }.map(_.toMap)

          whenReady(nodesState) { state =>
            println(s"************ nodes = $nodesState")
            verifySingleLeaderAndFollowers(state, allowFailed = false)
          }
        }
      }

      /*
      enterBarrier(s"waited-election")


      */

    }
 
  }

}
