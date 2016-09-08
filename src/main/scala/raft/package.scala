import akka.actor.ActorRef

package object raft {

  import akka.actor.ActorSelection


  type NodeId = Int
  type Term = Long
  type LogIndex = Long

  object ElectionTimeout

  // mostly types for tests:
  type Nodes = Map[NodeId, ActorSelection]
  type ClusterState = Map[NodeId, ActiveState]

}
