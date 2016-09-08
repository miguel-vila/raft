package raft

import akka.actor.{ ActorRef, ActorSystem }
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.util.Timeout
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object SampleMultiNodeConfig extends MultiNodeConfig {

  val askTimeout = Timeout(100.millis)

  val requestTimeout = 150.millis
  val heartbeatPeriod = 100.millis

  val electionTimeout : Map[NodeId, FiniteDuration] =
    Map(
      1 -> 150.millis,
      2 -> 225.millis,
      3 -> 275.millis,
      4 -> 250.millis,
      5 -> 300.millis
    )

  def name(nodeId: NodeId) = s"node$nodeId"

  val nodes: Map[NodeId, RoleName] = (for {
    id <- 1 to 5
  } yield {
    val nodeName = name(id)
    id -> role(nodeName)
  }).toMap

  def initialStateMachine(): IntNumberState = new IntNumberState()

  def createActorRef(nodeId: NodeId)(implicit actorSystem: ActorSystem): ActorRef = {
    NodeActor.createActor(nodeId, initialStateMachine(), electionTimeout(nodeId), heartbeatPeriod, requestTimeout, name(nodeId))
  }

}
