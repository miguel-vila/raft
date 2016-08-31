package raft

trait RPC[A] {
  def term: Term
}

case class RequestVoteRPC(
  term: Term,
  candidateId: NodeId,
  lastLogIndex: LogIndex,
  lastLogTerm: Term
) extends RPC[Nothing]

case class AppendEntriesRPC[A](
    term: Term,
    leaderId: NodeId,
    prevLogIndex: LogIndex,
    prevLogTerm: Term,
    entries: Vector[LogEntry[A]],
    leaderCommit: LogIndex
) extends RPC[A] {
  def isHeartbeat = entries.isEmpty
}

object Heartbeat {

  def apply(
    term: Term,
    leaderId: NodeId,
    prevLogIndex: LogIndex,
    prevLogTerm: Term,
    leaderCommit: LogIndex
  ): AppendEntriesRPC[Nothing] =
    AppendEntriesRPC(
      term = term,
      leaderId = leaderId,
      prevLogIndex = prevLogIndex,
      prevLogTerm = prevLogTerm,
      entries = Vector.empty,
      leaderCommit = leaderCommit
    )

}

case class RequestVoteResponse(
  term: Term,
  voteGranted: Boolean
)

case class AppendEntriesResponse(
  term: Term,
  success: Boolean
)
