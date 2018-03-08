import se.kth.edx.id2203.core.ExercisePrimitives.AddressUtils._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}
import scala.language.implicitConversions
import scala.collection.mutable.ListBuffer;

import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}




class Consensus extends Port{
   request[C_Propose];
   indication[C_Decide];
 }
 
 
 //
 case class C_Decide(value: Any) extends KompicsEvent;
 case class C_Propose(value: Any) extends KompicsEvent;
 
 
 
 
  case class Prepare(proposalBallot: (Int, Int)) extends KompicsEvent;
  case class Promise(promiseBallot: (Int, Int), acceptedBallot: (Int, Int), acceptedValue: Option[Any]) extends KompicsEvent;
  case class Accept(acceptBallot: (Int, Int), proposedValue: Option[Any]) extends KompicsEvent;
  case class Accepted(acceptedBallot: (Int, Int)) extends KompicsEvent;
  case class Nack(ballot: (Int, Int)) extends KompicsEvent;
  case class Decided(decidedValue: Option[Any]) extends KompicsEvent;

  /**
    * This augments tuples with comparison operators implicitly, which you can use in your code, for convenience. 
    * examples: (1,2) > (1,4) yields 'false' and  (5,4) <= (7,4) yields 'true' 
    */
  implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops = o.mkOrderingOps(x);
  
  //HINT: After you execute the latter implicit ordering you can compare tuples as such within your component implementation:
  (1,2) <= (1,4);
  
  
  
  
  
 
class Paxos(paxosInit: Init[Paxos]) extends ComponentDefinition {

  //Port Subscriptions for Paxos

  val consensus = provides[Consensus];
  val beb = requires[BestEffortBroadcast];
  val plink = requires[PerfectLink];
 
  //Internal State of Paxos
  val (rank, numProcesses) = paxosInit match {
    case Init(s: Address, qSize: Int) => (toRank(s), qSize)
  }
  val temp: Double = numProcesses + 1
  var majority: Integer = math.ceil( temp / 2).toInt

  //Proposer State
  var round = 0;
  var proposedValue: Option[Any] = None;
  var promises: ListBuffer[((Int, Int), Option[Any])] = ListBuffer.empty;
  var numOfAccepts = 0;
  var decided = false;

  //Acceptor State
  var promisedBallot = (0, 0);
  var acceptedBallot = (0, 0);
  var acceptedValue: Option[Any] = None;

  def propose() = {
   if (!decided) {
       
       round += 1
       numOfAccepts = 0
       promises = ListBuffer.empty
       //println(s"$rank proposing in round $round")
       trigger(BEB_Broadcast(Prepare((round,rank))) -> beb)
   }
  }

  consensus uponEvent {
    case C_Propose(value) => handle {
      proposedValue = Some(value)
      propose()
    }
  }


  beb uponEvent {

    case BEB_Deliver(src, Prepare(ballot)) => handle {
        if ( promisedBallot < ballot ) {
            promisedBallot = ballot
            //println(s"$rank - Prepare: sending Promise for $ballot to $src")
            trigger( PL_Send( src, Promise( promisedBallot, acceptedBallot, acceptedValue ) ) -> plink )
        } else {
            //println(s"$rank - Prepare: sending Nack for $ballot to $src")
            trigger( PL_Send( src, Nack( ballot ) ) -> plink )
        }
    };

    case BEB_Deliver(src, Accept( ballot, v ) ) => handle {
        if ( promisedBallot <= ballot ) {
            promisedBallot = ballot;
            acceptedBallot = ballot;
            acceptedValue = v;
            //println(s"$rank - Accept: sending Accepted for $ballot for $v to $src")
            trigger ( PL_Send( src, Accepted( ballot ) ) -> plink )
        } else {
            //println(s"$rank - Accept: sending Nack for $ballot to $src")
            trigger ( PL_Send( src, Nack( ballot ) ) -> plink )
        }
    };

    case BEB_Deliver(src, Decided( v )) => handle {
        if ( !decided ) {
            //val m = v.get
            //println(s"$rank - Decided: Value extracted dircetly is $m");
            trigger( C_Decide( v.get ) -> consensus )
            decided = true
        }
    }
  }

  plink uponEvent {

    case PL_Deliver(src, prepAck: Promise) => handle {
      if ((round, rank) == prepAck.promiseBallot) {
        promises += ( ( prepAck.acceptedBallot, prepAck.acceptedValue ) )
        //println(s"$rank - PL Promise: received Promise from $src;  majority = $majority")
        if ( promises.size ==  majority) {
            val (maxBallot, value: Option[Any]) = promises.maxBy(_._1)
            //println(s"$rank - PL Promise: proposed value = $proposedValue, value = $value")
            if(value.isDefined) {
                proposedValue = value
            }
            //println(s"$rank - PL Promise: broad Accept ( ($round, $rank), $proposedValue )")
            trigger(BEB_Broadcast( Accept( (round, rank), proposedValue ) ) -> beb)
        } 
      }
    };

    case PL_Deliver(src, accAck: Accepted) => handle {
      if ((round, rank) == accAck.acceptedBallot) {
        numOfAccepts += 1
        //println(s"$rank - PL Accept: received ack from $src; accepts = $numOfAccepts; N = $numProcesses")
        if ( numOfAccepts == majority ) {
            //println(s"$rank - PL Accept: broad Decided ( $proposedValue )")
            trigger( BEB_Broadcast( Decided( proposedValue ) ) -> beb)
        }
      }
    };

    case PL_Deliver(src, nack: Nack) => handle {
      if ((round, rank) == nack.ballot) {
        propose()
      }
    }
  }
  
 
};