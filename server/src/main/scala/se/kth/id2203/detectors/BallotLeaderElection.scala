package se.kth.id2203.detectors


import se.kth.id2203.messaging._
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start}

import scala.collection.mutable;


class BallotLeaderElection extends Port {
  indication[BLE_Leader];
  request[Monitor]
}

case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent;



//Provided Primitives to use in your implementation



case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;

case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;



class GossipLeaderElection extends ComponentDefinition {

  private val ballotOne = 0x0100000000l;

  val ble = provides[BallotLeaderElection];
  val pl = requires(PerfectLink);
  val timer = requires[Timer];

  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var topology = List[NetAddress]();
  val delta = cfg.getValue[Long]("id2203.project.keepAlivePeriod");
  var majority = (topology.size / 2) + 1;

  private var period = delta
  private val ballots = mutable.Map.empty[NetAddress, Long];

  private var round = 0l;
  private var ballot = ballotFromNAddress(0, self);

  private var leader: Option[(NetAddress, Long)] = None;
  private var highestBallot: Long = ballot;

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }


  private def checkLeader() {
    ballots += ( (self, ballot) )
    val top = ballots.maxBy(_._2)
    val ( topProcess, topBallot ) = top
    if ( topBallot < highestBallot ) {
      while ( ballot <= highestBallot ) {
        ballot = incrementBallotBy(ballot, 1)
      }
      leader = None
    } else {
      if( leader.isDefined ) {
        val l = leader.get
        if ( top != l ) {
          //println(self + " top != leader")
          highestBallot = topBallot
          leader = Some(top)
          trigger( BLE_Leader( topProcess, topBallot ) -> ble )
        }
      } else {
        //println(self + "leader was empty")
        highestBallot = topBallot
        leader = Some(top)
        trigger( BLE_Leader( topProcess, topBallot ) -> ble )
      }
    }
  }

  ble uponEvent {
    case Monitor(t) => handle {
      topology = t
      majority = (topology.size / 2) + 1;
      startTimer(period);
    }
  }

  ctrl uponEvent {
    case _: Start => handle {

    }
  }


  timer uponEvent {
    case CheckTimeout(_) => handle {
      val bSize = ballots.size;
      val tSize = topology.size;
      if ( ballots.size + 1 >= majority ) {
        //println (s"$self: $bSize + 1 >= $majority")
        //println( ballots )
        checkLeader();
      }
      //println(s"$self: before empty $ballots")
      ballots.clear
      //println(s"$self: after empty $ballots")
      round += 1
      for (p <- topology ) {
        if ( p != self ) {
          //println(self + ": requesting Hearbeat to " + p + " with bMax = " + highestBallot )
          trigger ( PL_Send( p, HeartbeatReq( round, highestBallot ) ) -> pl )
        }
      }
      startTimer(period)
    }
  }

  pl uponEvent {
    case PL_Deliver(src, HeartbeatReq(r, hb)) => handle {
      if ( hb > highestBallot ) {
        highestBallot = hb
      }
      trigger ( PL_Send( src, HeartbeatResp( r, ballot ) ) -> pl )
    }
    case PL_Deliver(src, HeartbeatResp(r, b)) => handle {
      if ( r == round) {
        //println(s"$self: Adding ballot for round $r with $b sent by $src")
        ballots += ( (src, b) )
      } else {
        //println(s"$self: round $r != $round")
        period += delta
      }
    }
  }


  def ballotFromNAddress(n: Int, adr: Address): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }

  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

}