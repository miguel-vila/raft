package raft

case class LogEntry[A](
  term: Term,
  index: LogIndex,
  data: A
)
