package raft

trait ClientRequest[A]
case class AppendEntry[A](entry: A) extends ClientRequest[A]
