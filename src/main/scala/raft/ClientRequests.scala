package raft

trait ClientRequest[A]
case class AppendEntryClientRequest[A](entry: A) extends ClientRequest[A]

case class AppendEntryClientResponse(response: Either[Throwable, AppendEntriesResponse], uncommitedIndex: LogIndex)
