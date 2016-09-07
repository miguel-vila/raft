package raft

trait NodeState
case object WaitingForNodeInfo extends NodeState
trait ActiveState extends NodeState {
  def term: Term
}
case class FailedNode(term: Term) extends ActiveState
case class Follower(leaderId: Option[NodeId], term: Term) extends ActiveState
case class Candidate(votes: Int, term: Term) extends ActiveState
case class Leader(nodeId: NodeId, term: Term) extends ActiveState

