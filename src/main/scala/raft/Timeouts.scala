package raft

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object Timeouts {

  def electionTimeout(): FiniteDuration = {
    val r = scala.util.Random
    val range = 150 to 300
    val numMillis = range(r.nextInt(range.length))
    numMillis.millis
  }

  val heartbeatTimeout: FiniteDuration = 300.millis

}
