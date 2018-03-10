package se.kth.id2203.kvstore


import se.kth.id2203.detectors.{BLE_Leader, BallotLeaderElection}
import se.kth.id2203.messaging._
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.sics.kompics.KompicsEvent

import scala.collection.mutable;




class SequenceConsensus extends Port {
  request[SC_Propose];
  request[Set_Topology]
  indication[SC_Decide];
}



case class SC_Propose(value: Operation) extends KompicsEvent;
case class SC_Decide(value: Operation) extends KompicsEvent;






//Provided Primitives to use in your implementation

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;
case class Promise(nL: Long, na: Long, suffix: List[Operation], ld: Int) extends KompicsEvent;
case class AcceptSync(nL: Long, suffix: List[Operation], ld: Int) extends KompicsEvent;
case class Accept(nL: Long, c: Operation) extends KompicsEvent;
case class Accepted(nL: Long, m: Int) extends KompicsEvent;
case class Decide(ld: Int, nL: Long) extends KompicsEvent;

object State extends Enumeration {
  type State = Value;
  val PREPARE, ACCEPT, UNKOWN = Value;
}

object Role extends Enumeration {
  type Role = Value;
  val LEADER, FOLLOWER = Value;
}



class SequencePaxos extends ComponentDefinition {

  import Role._
  import State._

  val sc = provides[SequenceConsensus];
  val ble = requires[BallotLeaderElection];
  val pl = requires(PerfectLink);

  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var pi = Set[NetAddress]()
  var others = Set[NetAddress]()
  var majority = (pi.size / 2) + 1;

  var stateNode = (FOLLOWER, UNKOWN);
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[NetAddress] = None;
  var na = 0l;
  var va = List.empty[Operation];
  var ld = 0;
  // leader state
  var propCmds = List.empty[Operation];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  var lc = 0;
  val acks = mutable.Map.empty[NetAddress, (Long, List[Operation])];

  private def max( map: mutable.Map[NetAddress, (Long, List[Operation])] ): (Long, List[Operation]) = {
    var highestRound: Long = 0;
    var v = List.empty[Operation]
    for ((_, tuple) <- map) {
      if( tuple._1 > highestRound ) {
        highestRound = tuple._1
        v = tuple._2
      } else if ( (tuple._1 == highestRound ) && ( tuple._2.size > v.size ) ) {
        highestRound = tuple._1
        v = tuple._2
      }
    }
    (highestRound, v)
  }

  ble uponEvent {
    case BLE_Leader(l, n) => handle {
      //println(s"$self: new leader event l = $l, n = $n")
      if ( n > nL ) {
        log.debug(s"$l is new leader")
        leader = Some ( l )
        nL = n
        if ( (self == l) && (nL > nProm) ) {
          //println(s"$self: I am leader for $pi")
          stateNode = (LEADER, PREPARE)
          propCmds = List.empty[Operation];
          for (p <- pi) {
            las += ( (p, 0) )
          }
          lds.clear
          acks.clear
          lc = 0
          for ( p <- others ) {
            trigger( PL_Send( p, Prepare( nL, ld, na ) ) -> pl )
          }
          acks += ( (l, (na, suffix( va, ld ))) )
          lds += ( (self, ld) )
          nProm = nL
        } else {
          stateNode = (FOLLOWER, stateNode._2)
        }
      }
    }
  }

  pl uponEvent {
    case PL_Deliver(p, Prepare(np, ldp, n)) => handle {
      //println(s"$self: received prepare from $p while my leader is $leader")
      if( nProm < np ) {
        nProm = np
        stateNode = (FOLLOWER, PREPARE)
        var sfx: List[Operation] = List.empty[Operation]
        if ( na >= n ) {
          sfx = suffix( va, ldp )
        }
        trigger( PL_Send( p, Promise( np, na, sfx, ld ) ) -> pl )
      }
    }
    case PL_Deliver(a, Promise(n, na, sfxa, lda)) => handle {
      if ((n == nL) && (stateNode == (LEADER, PREPARE))) {
        acks += ( (a, (na, sfxa)) )
        lds += ( (a, lda) )
        if( acks.size == majority ) {
          val (k, sfx) = max( acks )
          va = prefix( va, ld ) ++ sfx ++ propCmds
          las( self ) = va.size
          propCmds = List.empty[Operation]
          stateNode = (LEADER, ACCEPT)
          for ((p, v) <- lds ) {
            if( p != self ) {
              val sfxp = suffix( va, v)
              trigger( PL_Send( p, AcceptSync( nL, sfxp, v ) ) -> pl )
            }
          }
        }
      } else if ( ( n == nL ) && ( stateNode == (LEADER, ACCEPT) ) ) {
        lds += ( (a, lda) )
        val sfx = suffix( va, lds( a ) )
        trigger( PL_Send( a, AcceptSync( nL, sfx, lds ( a ) ) ) -> pl )
        if( lc != 0 ) {
          trigger( PL_Send( a, Decide( ld, nL) ) -> pl )
        }
      }
    }
    case PL_Deliver(p, AcceptSync(nL, sfx, ldp)) => handle {
      if ((nProm == nL) && (stateNode == (FOLLOWER, PREPARE))) {
        na = nL
        va = prefix( va, ldp ) ++ sfx
        trigger( PL_Send( p, Accepted( nL, va.size ) ) -> pl )
        stateNode = (FOLLOWER, ACCEPT)
      }
    }
    case PL_Deliver(p, Accept(nL, c)) => handle {
      if ((nProm == nL) && (stateNode == (FOLLOWER, ACCEPT))) {
        //if(va.size < 10) println(s"$self: received accept with value $c from $p")
        va = va :+ c
        trigger( PL_Send( p, Accepted( nL, va.size ) ) -> pl )
      }
    }
    case PL_Deliver(_, Decide(l, nL)) => handle {
      if( nProm == nL ) {
        while( ld < l ) {
          trigger( SC_Decide( va( ld ) ) -> sc )
          ld += 1
        }
        //println( self + ": decided sequence " + prefix( va, ld ) )
      }
    }
    case PL_Deliver(a, Accepted(n, m)) => handle {
      if ((n == nL) && (stateNode == (LEADER, ACCEPT))) {
        las( a ) = m
        if ( ( lc < m) && ( pi.filter( p => las( p ) >= m ).size >= majority ) ) {
          lc = m
          for ( (p, v) <- lds ) {
            trigger( PL_Send( p, Decide( lc, nL ) ) -> pl)
          }
        }

      }
    }
  }

  sc uponEvent {
    case SC_Propose(c) => handle {

      if (stateNode == (LEADER, PREPARE)) {
        propCmds = propCmds :+ c
      }
      else if (stateNode == (LEADER, ACCEPT)) {
        va = va :+ c
        las( self ) = las( self ) + 1
        //if(va.size < 10) println(s"$self: sending accept with value $c")
        for( (p, _) <- lds ) {
          if (p != self) {
            trigger( PL_Send( p, Accept( nL, c ) ) -> pl )
          }
        }
      } else {
        log.debug(s"Not leader; sending $c to $leader")
        trigger( PL_Send( leader.get, c) -> pl)
      }
    }
    case Set_Topology(topology) => handle {
      pi = topology
      others = pi - self
      majority = (pi.size / 2) + 1
      trigger(Monitor(pi.toList) -> ble)
    }
  }

  def suffix(s: List[Operation], l: Int): List[Operation] = {
    s.drop(l)
  }

  def prefix(s: List[Operation], l: Int): List[Operation] = {
    s.take(l)
  }
}
