package raft

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Cancellable, Props, Stash }
import scala.concurrent.Future
import scala.concurrent.duration.{ Duration, FiniteDuration }
import akka.pattern._
import akka.util.Timeout
import scala.util.Right

object NodeActor {

  def props[A, B](
    nodeId: NodeId,
    stateMachine: StateMachine[A, B],
    electionTimeout: FiniteDuration,
    heartbeatPeriod: FiniteDuration,
    requestTimeout: FiniteDuration
  ): Props = Props(
    new NodeActor(nodeId, stateMachine, electionTimeout, heartbeatPeriod, requestTimeout)
  )

  def createActor[A, B](
    nodeId: NodeId,
    stateMachine: StateMachine[A, B],
    electionTimeout: FiniteDuration,
    heartbeatPeriod: FiniteDuration,
    requestTimeout: FiniteDuration,
    name: String
  )(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(
      props(nodeId, stateMachine, electionTimeout, heartbeatPeriod, requestTimeout)
    , name)
  }

}

class NodeActor[A, B](
  nodeId: NodeId,
  stateMachine: StateMachine[A, B],
  electionTimeout: FiniteDuration,
  heartbeatPeriod: FiniteDuration,
  requestTimeout: FiniteDuration
) extends Actor with ActorLogging with Stash {
  require(heartbeatPeriod < electionTimeout)

  implicit val _requestTimeout = Timeout( requestTimeout )

  /*
   * State
   */
  var nodes: Map[NodeId, ActorSelection] = _

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

  /*
   * States
   */

  def receive = waitingForNodesInfo

  val waitingForNodesInfo: Receive = {
    case setNodesInfo: SetNodesInfo[A] =>
      nodes = setNodesInfo.nodes
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
    /*
    case unhandledMessage =>
      log.info(s"Node $nodeId received unhandled message while follower: $unhandledMessage")
      */
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
    /*
    case unhandledMessage =>
      log.info(s"Node $nodeId received unhandled message while candidate $unhandledMessage")
 */
  }

  var clientRequestsBuffer = Vector.empty[LogEntry[A]]

  def leader(nextIndex: Map[NodeId, LogIndex]): Receive = {
    case appendEntries: AppendEntriesRPC[A] =>
      if (appendEntries.term > currentTerm) { // @TODO greater than or greater or equal than?
        currentTerm = appendEntries.term
        cancelHeartbeatMessages()
        deleteUncommitedEntries()
        context.become(follower(leaderId = Some(appendEntries.leaderId)))
        self ! appendEntries // try to append the entries afterwards
      }
    case append: AppendEntryClientRequest[A] =>
      val uncommitedLogIndex = appendUncommitted(append.entry)
      Future.traverse(nodes) {
        case (otherNodeId,_) =>
          context.become(leaderWaitingForFollowerResponses(nextIndex))
          val nodeEntries = entriesLog.drop(logIndex) // @TODO check for off-by-one error
          //if (nextIndex(otherNodeId) <= logIndex) { // @TODO uncomment
          (nodes(otherNodeId) ? AppendEntriesRPC(
            term = currentTerm,
            leaderId = otherNodeId,
            prevLogIndex = nextIndex(otherNodeId),
            prevLogTerm = entriesLog(nextIndex(otherNodeId).toInt).term,
            entries = nodeEntries,
            leaderCommit = commitIndex
          )).mapTo[AppendEntriesResponse]
            .map( resp => Right(resp) )
            .recover{ case err: Throwable => Left(err) }
            .map( resp => SingleFollowerAppendResponse(otherNodeId, resp) )
          //}
      }.onSuccess { case responses =>
          self ! AppendEntriesFollowersResponse(responses.toList, uncommitedLogIndex)
      }
    case QueryState =>
      sender() ! Leader(nodeId, currentTerm)
    /*
    case unhandledMessage =>
      log.info(s"Received unhandled message while leader $unhandledMessage")
 */
  }

  case class SingleFollowerAppendResponse(
    nodeId: NodeId,
    response: Either[Throwable, AppendEntriesResponse]
  )

  case class AppendEntriesFollowersResponse(
    responses: List[SingleFollowerAppendResponse],
    uncommiteEntry: LogIndex
  ) {

    def positiveResponses(): List[SingleFollowerAppendResponse] = responses.collect {
      case x @ SingleFollowerAppendResponse(_,Right(response)) if response.success =>
        x
    }

  }

  def commitEntriesUpTo(index: LogIndex): Unit = {
    entriesLog
      .slice(commitIndex.toInt + 1, index.toInt + 1)
      .foreach { logEntry =>
        stateMachine.execute(logEntry.data)
    }
  }

  def leaderWaitingForFollowerResponses(nextIndex: Map[Int, Long]): Receive = {
    case responses: AppendEntriesFollowersResponse =>
      val positiveResponses = responses.positiveResponses()
      if(positiveResponses.size > nodes.size / 2) {
        commitEntriesUpTo(responses.uncommiteEntry)
      }
      unstashAll()
      context.become(leader(nextIndex))
    case _ =>
      stash()
  }

  /*
   * Methods
   */
  def startElectionTimeout() = {
    electionTimeoutMessage = system.scheduler.scheduleOnce(electionTimeout) {
      self ! ElectionTimeout
    }
  }

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
      case Some(currentLeader) if currentLeader == appendEntries.leaderId =>
        log.info(s"Node $nodeId received heartbeat from previous leader $appendEntries.leaderId, staying the same")
        resetElectionTimeout()
        sender() ! AppendEntriesResponse(term = currentTerm, true)
      case _ =>
        if (appendEntries.term >= currentTerm /* && checkConsistencyCondition(appendEntries)*/ ) {
          currentTerm = appendEntries.term
          resetElectionTimeout()
          log.info(s"Node $nodeId received heartbeat from a new leader $appendEntries.leaderId, setting leaderId")
          votedFor = None
          context.become(follower(leaderId = Some(appendEntries.leaderId)))
          sender() ! AppendEntriesResponse(term = currentTerm, true)
        } else {
          sender() ! AppendEntriesResponse(term = currentTerm, false)
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

  def appendUncommitted(entry: A): LogIndex = {
    val newEntry = LogEntry(term = currentTerm, index = entriesLog.length.toLong /*@TODO is this the correct index?*/ , data = entry)
    entriesLog = entriesLog :+ newEntry
    clientRequestsBuffer = clientRequestsBuffer :+ newEntry
    commitIndex
  }

  def requestVotes() = {
    nodes foreach {
      case (otherNodeId,_) =>
        nodes(otherNodeId) !
          RequestVoteRPC(
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
      val nextIndex: Map[NodeId, LogIndex] = nodes.map { case (nodeId,_) => nodeId -> (lastLogIndex + 1) }.toMap
      context.become(leader(nextIndex = nextIndex))
    } else {
      context.become(candidate(votesGranted))
    }
  }

  def sendLeaderHeartBeats() = {
    log.info(s"Node $nodeId sending leader heartbeats to other nodes")
    nodes foreach {
      case (otherNodeId,_) =>
        nodes(otherNodeId) !
          Heartbeat(
            term = currentTerm,
            leaderId = nodeId,
            prevLogIndex = lastLogTerm,
            prevLogTerm = lastLogTerm,
            leaderCommit = commitIndex
          ) //@TODO verify parameter values

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
      currentTerm = requestVote.term //??? do this even if vote is not granted?
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

  override def postStop() = {
    log.info(s"Executing post stop for $nodeId")
    if(electionTimeoutMessage != null) {
      electionTimeoutMessage.cancel()
    }
    if(heartbeatMessage != null) {
      heartbeatMessage.cancel()
    }

  }

}

//@TODO move this to another place
case class SetNodesInfo[A](nodes: Nodes)
case object QueryState
