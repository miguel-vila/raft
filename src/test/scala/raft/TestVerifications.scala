package raft

trait TestVerifications {

  def findLeaders(clusterState: ClusterState): List[(NodeId, ActiveState)] =
    clusterState.filter {
      case (_, nodeState) =>
        nodeState match {
          case Leader(_, _) => true
          case _ => false
        }
    }.toList

  /**
   * Verifies that the nodes are in a consistent state:
   *  There is only one leader and the rest of nodes are followers
   */
  def verifySingleLeaderAndFollowers(clusterState: ClusterState, allowFailed: Boolean = false): Boolean = {
    val leader = findLeaders(clusterState)
    leader match {
      case (leader :: _) =>
        val others = clusterState.filter(_ != leader)
        val (leaderId, Leader(_, leaderTerm)) = leader
        others.forall {
          case (_, nodeState) =>
            nodeState match {
              case Follower(leaderIdOpt, followerTerm) =>
                leaderIdOpt == Some(leaderId) && followerTerm == leaderTerm
              case FailedNode(_) =>
                allowFailed
              case _ => false
            }
        }
      case _ =>
        false
    }
  }

  def verifyAllInSameTerm(state: ClusterState, term: Term): Boolean =
    state.values.forall {
      case active: ActiveState => active.term == term
      case _ => false
    }

}
