//Rremember to execute the following imports first
import se.kth.edx.id2203.core.ExercisePrimitives._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{ComponentDefinition => _, Port => _,KompicsEvent}

import scala.collection.mutable.Map
import scala.language.implicitConversions

 class AtomicRegister extends Port {
   request[AR_Read_Request]
   request[AR_Write_Request]
   indication[AR_Read_Response]
   indication[AR_Write_Response]
 }
 
  case class AR_Read_Request() extends KompicsEvent
 case class AR_Read_Response(value: Option[Any]) extends KompicsEvent
 case class AR_Write_Request(value: Any) extends KompicsEvent
 case class AR_Write_Response() extends KompicsEvent
 
 
 
 
   //The following events are to be used internally by the Atomic Register implementation below
  case class READ(rid: Int) extends KompicsEvent;
  case class VALUE(rid: Int, ts: Int, wr: Int, value: Option[Any]) extends KompicsEvent;
  case class WRITE(rid: Int, ts: Int, wr: Int, writeVal: Option[Any]) extends KompicsEvent;
  case class ACK(rid: Int) extends KompicsEvent;

  /**
    * This augments tuples with comparison operators implicitly, which you can use in your code. 
    * examples: (1,2) > (1,4) yields 'false' and  (5,4) <= (7,4) yields 'true' 
    */
  implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.Ops = o.mkOrderingOps(x);

//HINT: After you execute the latter implicit ordering you can compare tuples as such within your component implementation:
(1,2) <= (1,4);






class ReadImposeWriteConsultMajority(init: Init[ReadImposeWriteConsultMajority]) extends ComponentDefinition {

  //subscriptions

  val nnar = provides[AtomicRegister];

  val pLink = requires[PerfectLink];
  val beb = requires[BestEffortBroadcast];

  //state and initialization

  val (self: Address, n: Int, selfRank: Int) = init match {
    case Init(selfAddr: Address, n: Int) => (selfAddr, n, AddressUtils.toRank(selfAddr))
  };

  var (ts, wr) = (0, 0);
  var value: Option[Any] = None;
  var acks = 0;
  var readval: Option[Any] = None;
  var writeval: Option[Any] = None;
  var rid = 0;
  var readlist: Map[Address, (Int, Int, Option[Any])] = Map.empty
  var reading = false;

  //handlers

  nnar uponEvent {
    case AR_Read_Request() => handle {
      rid = rid + 1;
      acks = 0
      readlist = Map.empty
      reading = true;
      //println( s"$selfRank: starting read for READ w/ id $rid")
      trigger (BEB_Broadcast(READ(rid)) -> beb)
     
    }
    case AR_Write_Request(wval) => handle { 
      rid = rid + 1;
      writeval = Some(wval)
      acks = 0
      readlist = Map.empty
      //println( s"$selfRank: starting read for WRITE with id $rid")
      trigger(BEB_Broadcast(READ(rid)) -> beb)
     
    }
  }

  beb uponEvent {
    case BEB_Deliver(src, READ(readID)) => handle {
        //println( s"$selfRank: sending id = $readID, ts = $ts, wr = $wr, value = $value to $src")
        trigger(PL_Send(src, VALUE(readID, ts, wr, value)) -> pLink)
     
    }
    case BEB_Deliver(src, WRITE(r, ts1, wr1, v1)) => handle {
      if((ts1, wr1) > (ts, wr)) {
        //println( s"$selfRank: saving ($ts1, $wr1) > ($ts, $wr) and value = $v1") 
        ts = ts1
        wr = wr1
        value = v1
      }
      //println( s"$selfRank: sending ACK of id $r to $src")
      trigger(PL_Send(src, ACK(r)) -> pLink)
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, v: VALUE) => handle {
      if (v.rid == rid) {
        readlist += ( (src, (v.ts, v.wr, v.value)) )
        //println( s"$selfRank: readlist = $readlist")
        if(readlist.size > (n/2)) {
            var (maxts, rr, temp) = highest(readlist)
            readval = temp
            readlist = Map.empty
            var bcastval: Option[Any] = None
            if (reading) bcastval = readval
            else {
                rr = selfRank
                maxts += 1
                bcastval = writeval
            }
            //println( s"$selfRank: triggering broadcast WRITE id = $rid, maxts = $maxts, rank = $rr, bcastval = $bcastval")
            trigger(BEB_Broadcast(WRITE(rid, maxts, rr, bcastval)) -> beb)
        }
     
      }
    }
    case PL_Deliver(src, v: ACK) => handle {
      if (v.rid == rid) {
        acks += 1
        if (acks > (n/2)) {
            acks = 0
            if (reading) {
                reading = false
                //println( s"$selfRank: providing AR_Read_response w/ $readval")
                trigger(AR_Read_Response(readval) -> nnar)
            } else {
                //println( s"$selfRank: providing AR_Write_response")
                trigger(AR_Write_Response() -> nnar)
            }
        }
     
      }
    }
  }
  
  def highest(list: Map[Address, (Int, Int, Option[Any])]): (Int, Int, Option[Any]) = {
      var t:Int = 0
      var w:Int = 0
      var v:Option[Any] = None
      for ( (_, tup) <- list) {
        if (tup._1 > t) {
            t = tup._1
            w = tup._2
            v = tup._3
        } else if (tup._1 == ts && tup._2 > w) {
            t = tup._1
            w = tup._2
            v = tup._3
        }
      }
      //println( s"$selfRank: returning ($t, $w, $v)")
      (t, w, v)  
  }
}