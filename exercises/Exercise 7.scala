import se.kth.edx.id2203.core.ExercisePrimitives.AddressUtils._
import se.kth.edx.id2203.core.ExercisePrimitives.AddressUtils
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.kth.edx.id2203.core.Ports.{SequenceConsensus, _}
import se.sics.kompics.KompicsEvent
import se.kth.edx.id2203.validation._

import scala.collection.mutable;




class SequenceConsensus extends Port {
    request[SC_Propose];
    indication[SC_Decide];
}



case class SC_Propose(value: RSM_Command) extends KompicsEvent;
case class SC_Decide(value: RSM_Command) extends KompicsEvent;

trait RSM_Command






//Provided Primitives to use in your implementation

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;
case class Promise(nL: Long, na: Long, suffix: List[RSM_Command], ld: Int) extends KompicsEvent;
case class AcceptSync(nL: Long, suffix: List[RSM_Command], ld: Int) extends KompicsEvent;
case class Accept(nL: Long, c: RSM_Command) extends KompicsEvent;
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

def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
    s.drop(l)
}

def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
    s.take(l)
}






class SequencePaxos(init: Init[SequencePaxos]) extends ComponentDefinition {

import Role._
import State._
    
  val sc = provides[SequenceConsensus];
  val ble = requires[BallotLeaderElection];
  val pl = requires[FIFOPerfectLink];

  val (self, pi, others) = init match {
    case Init(addr: Address, pi: Set[Address] @unchecked) => (addr, pi, pi - addr)
  }
  val majority = (pi.size / 2) + 1;

  var state = (FOLLOWER, UNKOWN);
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[Address] = None;
  var na = 0l;
  var va = List.empty[RSM_Command];
  var ld = 0;
  // leader state
  var propCmds = List.empty[RSM_Command];
  val las = mutable.Map.empty[Address, Int];
  val lds = mutable.Map.empty[Address, Int];
  var lc = 0;
  val acks = mutable.Map.empty[Address, (Long, List[RSM_Command])];

  private def max( map: mutable.Map[Address, (Long, List[RSM_Command])] ): (Long, List[RSM_Command]) = {
      var highestRound: Long = 0;
      var v = List.empty[RSM_Command]
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
            //println(s"$self: new leader event n > $nL")
            leader = Some ( l )
            nL = n
            if ( (self == l) && (nL > nProm) ) {
                //println(s"$self: I am leader now")
                state = (LEADER, PREPARE)
                propCmds = List.empty[RSM_Command];
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
                state = (FOLLOWER, state._2)
            }
        }
    }
   }
   
  pl uponEvent {
    case PL_Deliver(p, Prepare(np, ldp, n)) => handle {
        //println(s"$self: received prepare from $p while my leader is $leader")
        if( nProm < np ) {
            nProm = np
            state = (FOLLOWER, PREPARE)
            var sfx: List[RSM_Command] = List.empty[RSM_Command]
            if ( na >= n ) {
                sfx = suffix( va, ldp )
            }
            trigger( PL_Send( p, Promise( np, na, sfx, ld ) ) -> pl )
        }
    }
    case PL_Deliver(a, Promise(n, na, sfxa, lda)) => handle {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += ( (a, (na, sfxa)) )
        lds += ( (a, lda) )
        if( acks.size == majority ) {
            val (k, sfx) = max( acks )
            va = prefix( va, ld ) ++ sfx ++ propCmds
            las( self ) = va.size
            propCmds = List.empty[RSM_Command]
            state = (LEADER, ACCEPT)
            for ((p, v) <- lds ) {
                if( p != self ) {
                    val sfxp = suffix( va, v)
                    trigger( PL_Send( p, AcceptSync( nL, sfxp, v ) ) -> pl )
                }
            }
        }
      } else if ( ( n == nL ) && ( state == (LEADER, ACCEPT) ) ) {
        lds += ( (a, lda) )
        val sfx = suffix( va, lds( a ) )
        trigger( PL_Send( a, AcceptSync( nL, sfx, lds ( a ) ) ) -> pl )
        if( lc != 0 ) {
            trigger( PL_Send( a, Decide( ld, nL) ) -> pl )
        }
      }
    }
    case PL_Deliver(p, AcceptSync(nL, sfx, ldp)) => handle {
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        na = nL
        va = prefix( va, ldp ) ++ sfx
        trigger( PL_Send( p, Accepted( nL, va.size ) ) -> pl )
        state = (FOLLOWER, ACCEPT)
      }
    }
    case PL_Deliver(p, Accept(nL, c)) => handle {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
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
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
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
        
      if (state == (LEADER, PREPARE)) {
        propCmds = propCmds :+ c
      } 
      else if (state == (LEADER, ACCEPT)) {
        va = va :+ c
        las( self ) = las( self ) + 1
        //if(va.size < 10) println(s"$self: sending accept with value $c")
        for( (p, _) <- lds ) {
            if (p != self) {
                trigger( PL_Send( p, Accept( nL, c ) ) -> pl )
            }
        }
      }
    }
  }
}