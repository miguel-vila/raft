package raft

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern._
import akka.util.Timeout
import scala.collection.mutable.SortedSet
import scala.concurrent.Future
import scala.collection.mutable.Set

class ActorNetwork[A](nodes: Map[NodeId, ActorRef])(implicit actorSystem: ActorSystem) extends Network[A] {

  val networkPartitions: Set[(NodeId, NodeId)] = SortedSet()

  private def identify(sender: ActorRef): NodeId = {
    nodes
      .find { case (nodeId,nodeRef) => nodeRef == sender }
      .map { case (nodeId,_) => nodeId }
    .getOrElse(throw new Exception(s"The sender $sender doesn't correspond to any node in the system"))
  }

  def sendTo(nodeId: NodeId, rpc: RPC[A])(implicit sender: ActorRef): Unit = {
    //println(s"sending message from ${identify(sender)} to node $nodeId")
    val senderId = identify(sender)
    if(!existsNetworkPartition(nodeId, senderId)) {
      nodes(nodeId) ! rpc
    } else {
      println(s"Failed message sending from node $senderId to node $nodeId. Message: $rpc")
    }
  }

  def respond(node: ActorRef, rpcResponse: RPCResponse)(implicit sender: ActorRef): Unit = {
    val nodeId = identify(node)
    val senderId = identify(sender)
    if(!existsNetworkPartition(nodeId, senderId)) {
      node ! rpcResponse
    } else {
      println(s"Failed message sending from node $senderId to node $nodeId. Message: $rpcResponse")
    }
  }

  def ask(nodeId: NodeId, rpc: RPC[A])(implicit timeout: Timeout, sender: ActorRef): Future[Any] = {
    val senderId = identify(sender)
    if(!existsNetworkPartition(nodeId, senderId)) {
      new AskableActorRef(nodes(nodeId)) ? rpc
    } else {
      val message = s"Failed message sending from node $senderId to node $nodeId. Message: $rpc"
      println(message)
      Future.failed( new Exception(message) )
    }
  }

  def existsNetworkPartition(nodeA: NodeId, nodeB: NodeId): Boolean = {
    networkPartitions.contains( Math.min(nodeA,nodeB) -> Math.max(nodeA,nodeB) )
  }

  def createNetworkPartition(nodeA: NodeId, nodeB: NodeId): Unit = {
    assert(nodeA != nodeB)
    networkPartitions.add( Math.min(nodeA,nodeB) -> Math.max(nodeA,nodeB) )
  }

  def stopNode(nodeId: NodeId): Unit = {
    actorSystem.stop( nodes(nodeId) )
  }

}
