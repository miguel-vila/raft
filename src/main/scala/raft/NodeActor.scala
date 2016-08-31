package raft

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, Cancellable, Props, Stash }
import scala.concurrent.duration.{ Duration, FiniteDuration }

object NodeActor {

  def props[A, B](
    nodeId: NodeId,
    stateMachine: StateMachine[A, B],
    electionTimeout: FiniteDuration,
    heartbeatPeriod: FiniteDuration
  ): Props = Props(
    new NodeActor(nodeId, stateMachine, electionTimeout, heartbeatPeriod)
  )

  def createActor[A, B](
    nodeId: NodeId,
    stateMachine: StateMachine[A, B],
    electionTimeout: FiniteDuration,
    heartbeatPeriod: FiniteDuration
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(
      props(nodeId, stateMachine, electionTimeout, heartbeatPeriod)
    )
  }

}

class NodeActor[A, B](
    nodeId: NodeId,
    stateMachine: StateMachine[A, B],
    electionTimeout: FiniteDuration,
    heartbeatPeriod: FiniteDuration
) extends Actor with ActorLogging with Stash {
  require(heartbeatPeriod < electionTimeout)

  /*
   * State
   */

  var nodes: Map[NodeId, ActorRef] = _

  var currentTerm: Term = 0 //should be persistent
  var votedFor: Option[NodeId] = None //should be persistent

  var entriesLog: Vector[LogEntry[A]] = Vector.empty //should be persistent
  def logIndex = entriesLog.length - 1

  def lastLogIndex: LogIndex = if (entriesLog.isEmpty) -1 else entriesLog.last.index
  def lastLogTerm: Term = if (entriesLog.isEmpty) -1 else entriesLog.last.term

  val system = context.system
  import system.dispatcher

  // volatile data:
  var electionTimeoutMessage: Cancellable = _
  var heartbeatMessage: Cancellable = _
  var commitIndex: LogIndex = 0
  var lastApplied: LogIndex = 0

  def startElectionTimeout() = {
    electionTimeoutMessage = system.scheduler.scheduleOnce(electionTimeout) {
      self ! ElectionTimeout
    }
  }

  /*
   * States
   */

  def receive = waitingForNodesInfo

  val waitingForNodesInfo: Receive = {
    case SetNodesInfo(nodesInfo) =>
      nodes = nodesInfo
      unstashAll()
      context.become(startupState())
    case QueryState =>
      sender() ! WaitingForNodeInfo
    case message =>
      stash()
  }

  def startupState(): Receive = {
    startElectionTimeout()
    follower(leaderId = None)
  }

  def follower(leaderId: Option[NodeId]): Receive = {
    case ElectionTimeout =>
      log.info(s"Node $nodeId timed out for election while follower with leader $leaderId")
      startNewElection()
    case requestVote: RequestVoteRPC =>
      handleVoteRequest(requestVote, sender())
    case appendEntries: AppendEntriesRPC[A] if appendEntries.isHeartbeat =>
      log.info(s"Node $nodeId received heartbeat from node ${appendEntries.leaderId} while follower")
      handleHeartbeat(leaderId, appendEntries)
    case appendEntries: AppendEntriesRPC[A] =>
      leaderId match {
        case Some(leaderId) =>
        case None =>
      }
      if (appendEntries.term < currentTerm || !checkConsistencyCondition(appendEntries)) {
        sender() ! AppendEntriesResponse(term = currentTerm, success = false)
      } else {
        resetElectionTimeout() // @TODO should also reset in the previous clause
        val conflictPoint = getConflictPoint(appendEntries)
        conflictPoint foreach deleteEntriesStartingOn
        val newEntries = getNewEntries(appendEntries.entries)
        entriesLog ++= newEntries
        if (appendEntries.leaderCommit > commitIndex) {
          commitIndex = Math.min(appendEntries.leaderCommit, newEntries.last.index)
        }
      }
    case QueryState =>
      sender() ! Follower(leaderId, currentTerm)
    case unhandledMessage =>
      log.info(s"Node $nodeId received unhandled message while follower: $unhandledMessage")
  }

  def candidate(votesGranted: Int): Receive = {
    case requestVote: RequestVoteRPC =>
      handleVoteRequest(requestVote, sender())
    case RequestVoteResponse(term, voteGranted) =>
      if (voteGranted) {
        log.info(s"Node $nodeId received vote for term $term")
        verifyMajorityOfVotes(votesGranted = votesGranted + 1)
      } else {
        log.info(s"Node $nodeId got vote rejected for term $term")
      }
    case appendEntries: AppendEntriesRPC[A] if appendEntries.isHeartbeat =>
      log.info(s"Node $nodeId received heartbeat while candidate")
      handleHeartbeat(leaderId = None, appendEntries)
    case appendEntries: AppendEntriesRPC[A] =>
      if (appendEntries.term >= currentTerm) { //@TODO is it greater or equal or just greater?
        context.become(follower(leaderId = Some(appendEntries.leaderId)))
        currentTerm = appendEntries.term
        self ! appendEntries
      }
    case ElectionTimeout =>
      log.info(s"Node $nodeId timed out for election while candidate")
      startNewElection()
    case QueryState =>
      sender() ! Candidate(votesGranted, currentTerm)
    case unhandledMessage =>
      log.info(s"Node $nodeId received unhandled message while candidate $unhandledMessage")
  }

  def leader(nextIndex: Map[NodeId, LogIndex]): Receive = {
    case appendEntries: AppendEntriesRPC[A] =>
      if (appendEntries.term > currentTerm) { // @TODO greater than or greater or equal than?
        currentTerm = appendEntries.term
        cancelHeartbeatMessages()
        deleteUncommitedEntries()
        context.become(follower(leaderId = Some(appendEntries.leaderId)))
        self ! appendEntries // try to append the entries afterwards
      }
    case append: AppendEntry[A] =>
      appendUncommitted(append.entry)
      nodes foreach {
        case (nodeId, node) =>
          if (nextIndex(nodeId) <= logIndex) {
            val nodeEntries = entriesLog.drop(logIndex) // @TODO check for off-by-one error
            node ! AppendEntriesRPC(
              term = currentTerm,
              leaderId = nodeId,
              prevLogIndex = nextIndex(nodeId),
              prevLogTerm = entriesLog(nextIndex(nodeId).toInt).term,
              entries = nodeEntries, leaderCommit = commitIndex
            )
            // @TODO retries when append fails
          }
      }
    case QueryState =>
      sender() ! Leader(nodeId, currentTerm)
    case unhandledMessage =>
      log.info(s"Received unhandled message while leader $unhandledMessage")
  }

  /*
   * Methods
   */
  def startNewElection() = {
    log.info(s"Node $nodeId starting new election for term $currentTerm")
    currentTerm += 1
    votedFor = Some(nodeId)
    requestVotes()
    startElectionTimeout()
    context.become(candidate(votesGranted = 1))
  }

  def cancelElectionTimeout() = {
    electionTimeoutMessage.cancel() // @TODO verify return value?    
  }

  def resetElectionTimeout() = {
    cancelElectionTimeout()
    startElectionTimeout()
  }

  def checkConsistencyCondition(appendEntries: AppendEntriesRPC[A]): Boolean = {
    entriesLog.lift(appendEntries.prevLogIndex.toInt)
      .map(_.term == appendEntries.prevLogTerm)
      .getOrElse(false)
  }

  def getConflictPoint(appendEntries: AppendEntriesRPC[A]): Option[LogIndex] = {
    entriesLog.find { logEntry =>
      appendEntries.entries.exists { newEntry =>
        newEntry.term != logEntry.term
      } // @TODO more efficient way to do this?
    }.map(_.index)
  }

  def deleteEntriesStartingOn(index: LogIndex): Unit = {
    entriesLog = entriesLog.takeWhile(_.index < index)
  }

  def getNewEntries(entries: Vector[LogEntry[A]]): Vector[LogEntry[A]] = {
    entries.filter { entry =>
      !entriesLog.exists { logEntry =>
        logEntry.term == entry.term && logEntry.index == entry.index
      }
    }
  }

  def handleHeartbeat(leaderId: Option[NodeId], appendEntries: AppendEntriesRPC[A]): Unit = {
    leaderId match {
      case None =>
        resetElectionTimeout()
        log.info(s"Node $nodeId received heartbeat from a new leader $appendEntries.leaderId, setting leaderId")
        context.become(follower(leaderId = Some(appendEntries.leaderId)))
        sender() ! AppendEntriesResponse(term = currentTerm, true)
      case Some(currentLeader) =>
        if (currentLeader == appendEntries.leaderId) {
          log.info(s"Node $nodeId received heartbeat from previous leader $appendEntries.leaderId, staying the same")
          resetElectionTimeout()
          sender() ! AppendEntriesResponse(term = currentTerm, true)
        } else {
          log.info("WTF? split brain?") //@TODO handle case
        }
    }
  }

  def startHeartbeatMessages(): Unit = {
    heartbeatMessage = system.scheduler.schedule(Duration.Zero, heartbeatPeriod) {
      sendLeaderHeartBeats()
    }
  }

  def deleteUncommitedEntries() = {
    entriesLog = entriesLog.take(commitIndex.toInt + 1)
  }

  def cancelHeartbeatMessages() = {
    heartbeatMessage.cancel()
  }

  def appendUncommitted(entry: A): Unit = {
    entriesLog = entriesLog :+ LogEntry(term = currentTerm, index = entriesLog.length.toLong /*@TODO is this the correct index?*/ , data = entry)
  }

  def requestVotes() = {
    nodes foreach {
      case (_, node) =>
        node ! RequestVoteRPC(
          term = currentTerm,
          candidateId = nodeId,
          lastLogIndex = lastLogIndex,
          lastLogTerm = lastLogTerm
        )
    }
  }

  def verifyMajorityOfVotes(votesGranted: Int) = {
    log.info(s"Node $nodeId verifying majority of the votes: votes granted = $votesGranted")
    if (votesGranted > nodes.size / 2) { //@TODO verify if this is the correct predicate
      log.info(s"Node $nodeId got majority of votes, becoming leader")
      cancelElectionTimeout()
      startHeartbeatMessages()
      val nextIndex: Map[NodeId, LogIndex] = nodes.mapValues(_ => lastLogIndex + 1)
      context.become(leader(nextIndex = nextIndex))
    } else {
      context.become(candidate(votesGranted))
    }
  }

  def sendLeaderHeartBeats() = {
    log.info(s"Node $nodeId sending leader heartbeats to other nodes")
    nodes foreach {
      case (_, node) =>
        node ! Heartbeat(term = currentTerm, leaderId = nodeId, prevLogIndex = lastLogTerm, prevLogTerm = lastLogTerm, leaderCommit = commitIndex) //@TODO verify parameter values
    }
  }

  def logIsAtLeastUpToDate(otherLastLogIndex: LogIndex, otherLastLogTerm: Term): Boolean = {
    // page 8 of paper, last parragraph of section 5.4.1
    if (lastLogTerm == otherLastLogTerm) {
      lastLogIndex >= otherLastLogIndex
    } else {
      lastLogTerm >= otherLastLogTerm
    }
  }

  def handleVoteRequest(requestVote: RequestVoteRPC, candidate: ActorRef): Unit = {
    log.info(s"Node $nodeId handling vote request from node ${requestVote.candidateId}")
    if (requestVote.term >= currentTerm) {
      currentTerm = requestVote.term
      votedFor match {
        case Some(previousVote) =>
          log.info(s"Node $nodeId rejecting vote for node ${requestVote.candidateId} for term $currentTerm because already voted for $previousVote")
          candidate ! RequestVoteResponse(currentTerm, false)
        case None =>
          log.info(s"Verifying if $nodeId's log is up to date with respect to ${requestVote.candidateId}")
          if (logIsAtLeastUpToDate(requestVote.lastLogIndex, requestVote.lastLogTerm)) {
            log.info(s"$nodeId's log is up to date w.r.t ${requestVote.candidateId}")
            resetElectionTimeout() // @TODO is this the correct place for this?
            log.info(s"Node $nodeId voting for node ${requestVote.candidateId} for term $currentTerm")
            candidate ! RequestVoteResponse(currentTerm, true)
            votedFor = Some(requestVote.candidateId)
          } else {
            log.info(s"Node $nodeId rejecting vote for node ${requestVote.candidateId} for term $currentTerm because logs are not up to date")
            candidate ! RequestVoteResponse(currentTerm, false)
          }
      }
    } else {
      log.info(s"Node $nodeId rejecting vote for node ${requestVote.candidateId} for term $currentTerm because candidate's term is old")
      candidate ! RequestVoteResponse(currentTerm, false)
    }
  }

}

//@TODO move this to another place
case class SetNodesInfo(nodesInfo: Map[NodeId, ActorRef])
case object QueryState
