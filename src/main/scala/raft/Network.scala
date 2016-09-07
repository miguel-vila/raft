package raft

import akka.actor.ActorRef
import akka.util.Timeout
import scala.concurrent.Future

trait Network[A] {

  def sendTo(nodeId: NodeId, rpc: RPC[A])(implicit sender: ActorRef): Unit

  def respond(node: ActorRef, rpcResponse: RPCResponse)(implicit sender: ActorRef): Unit

  def ask(nodeId: NodeId, rpc: RPC[A])(implicit timeout: Timeout, sender: ActorRef): Future[Any]

}
