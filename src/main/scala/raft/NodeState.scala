package raft

trait NodeState
case object WaitingForNodeInfo extends NodeState
case class Follower(leaderId: Option[NodeId], term: Term) extends NodeState
case class Candidate(votes: Int, term: Term) extends NodeState
case class Leader(nodeId: NodeId, term: Term) extends NodeState

