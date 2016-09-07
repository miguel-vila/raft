package raft

import akka.actor.ActorSystem
import akka.util.Timeout
import org.scalatest._
import org.scalatest.time.{ Millis, Seconds, Span }
import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.concurrent.ScalaFutures
import scala.util.{ Failure, Success }

class NodeActorSpec extends FlatSpec with Matchers with ScalaFutures with TestUtils with TestVerifications {

  implicit val timeout = Timeout(100.millis)
  implicit val patience =
      PatienceConfig(timeout = Span(1, Seconds), interval = Span(5, Millis))

  
  "NodeActor" should "elect as a leader the first node that starts an election" in {
    implicit val actorSystem: ActorSystem = ActorSystem("test-system-1")
    implicit val ec = actorSystem.dispatcher
    val nodes = setupSimpleNodeSystem()
    Thread.sleep(750)


    whenReady(
      for {
        state <- getNodesState(nodes)
        _ <- actorSystem.terminate()
      } yield state
    ){ state =>
      verifySingleLeaderAndFollowers(state) should equal(true)
      verifyAllInSameTerm(state, term = 1) should equal(true)
    }

  }  

  it should "elect a new leader after a leader failure" in {
    implicit val actorSystem = ActorSystem("test-system-2")
    implicit val ec = actorSystem.dispatcher
    val nodes = setupSimpleNodeSystem()
    Thread.sleep(750)
    val afterMakingLeaderFail = for {
      state <- getNodesState(nodes)
    } yield makeLeadersFail(nodes, state, verifyNumberOfLeaders = Some(1))
    val afterWaitingElection = afterMakingLeaderFail.map { _ =>
      Thread.sleep(750)
    }
    val stateAfterFailureF = for {
      _ <- afterWaitingElection
      state <- getNodesState(nodes)
      _ <- actorSystem.terminate()
    } yield state

    whenReady(stateAfterFailureF) { state =>
      val leaders = findLeaders(state)
      leaders.size should equal(1)
      val (newLeaderId,newLeaderState) = leaders.head
      newLeaderState.term should equal (2)
      state.forall {
        case (_,Follower(leaderIdOpt, followerTerm)) =>
          leaderIdOpt == Some(newLeaderId) &&
          newLeaderState.term == followerTerm
        case (_,Candidate(_,_)) =>
          false
        case _ =>
          true
      } should equal(true)
    }
  }

}
