import akka.actor.ActorRef

package object raft {

  type NodeId = Int
  type Term = Long
  type LogIndex = Long

  object ElectionTimeout

  // mostly types for tests:
  type Nodes = Map[NodeId, ActorRef]
  type ClusterState = Map[NodeId, ActiveState]

}
