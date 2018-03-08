import se.kth.edx.id2203.core.ExercisePrimitives._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{ComponentDefinition => _, Port => _, KompicsEvent}

import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer



class BasicBroadcast(init: Init[BasicBroadcast]) extends ComponentDefinition {

  //subscriptions
  val pLink = requires[PerfectLink];
  val beb = provides[BestEffortBroadcast];

  //configuration
  val (self, topology) = init match {
    case Init(s: Address, t: Set[Address]@unchecked) => (s, t)
  };

  //handlers
  beb uponEvent {
    case x: BEB_Broadcast => handle {
      for (q <- topology) {
          trigger(PL_Send(q, x) -> pLink)
      }
     
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, BEB_Broadcast(payload)) => handle {
      trigger(BEB_Deliver(src, payload) -> beb)
     
    }
  }
}







class ReliableBroadcast extends Port {
  indication[RB_Deliver];
  request[RB_Broadcast];
}

case class RB_Deliver(source: Address, payload: KompicsEvent) extends KompicsEvent;
case class RB_Broadcast(payload: KompicsEvent) extends KompicsEvent;


//Reliable Broadcast

case class OriginatedData(src: Address, payload: KompicsEvent) extends KompicsEvent;

class EagerReliableBroadcast(init: Init[EagerReliableBroadcast]) extends ComponentDefinition {
  
  //EagerReliableBroadcast Subscriptions
  val beb = requires[BestEffortBroadcast];
  val rb = provides[ReliableBroadcast];

  //EagerReliableBroadcast Component State and Initialization
  val self = init match {
    case Init(s: Address) => s
  };
  val delivered = collection.mutable.Set[KompicsEvent]();

  //EagerReliableBroadcast Event Handlers
  rb uponEvent {
    case x@RB_Broadcast(payload) => handle {
     
     trigger(BEB_Broadcast(OriginatedData(self, payload)) -> beb)
     
    }
  }

  beb uponEvent {
    case BEB_Deliver(_, data@OriginatedData(origin, payload)) => handle {
     if (!delivered.contains(payload)) {
         delivered += payload
         trigger(RB_Deliver(origin, payload)-> rb)
         trigger(BEB_Broadcast(data) -> beb)
     }
     
     
    }
  }
}






class CausalOrderReliableBroadcast extends Port {
  indication[CRB_Deliver];
  request[CRB_Broadcast];
}

case class CRB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
case class CRB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

//vectocklock implementation can be found on their gist https://gist.github.com/senorcarbone/5c960ee27a67ec8b6bd42c33303fdcd2

//Causal Reliable Broadcast

case class DataMessage(timestamp: VectorClock, payload: KompicsEvent) extends KompicsEvent;

class WaitingCRB(init: Init[WaitingCRB]) extends ComponentDefinition {

  //subscriptions
  val rb = requires[ReliableBroadcast];
  val crb = provides[CausalOrderReliableBroadcast];

  //configuration
  val (self, vec) = init match {
    case Init(s: Address, t: Set[Address]@unchecked) => (s, VectorClock.empty(t.toSeq))
  };

  //state
  var pending: ListBuffer[(Address, DataMessage)] = ListBuffer();
  var lsn:Integer = 0;


  //handlers
  crb uponEvent {
    case CRB_Broadcast(payload) => handle {
      var w = VectorClock(vec);
      w.set(self, lsn);
      //println(self + ": vectorclock = " + w + " lsn = " + lsn);
      lsn += 1;
      println(self + ": sending " + payload + " with vector" + w);
      trigger(RB_Broadcast(DataMessage(w, payload)) -> rb);
     
    }
  }

  rb uponEvent {
    case x@RB_Deliver(src: Address, msg: DataMessage) => handle {
        //println(self + ": adding delivery" + x);
        
        pending += ((src, msg));
        var change = true;
        while(change) {
            change = false;
            pending.foreach{
                pend => 
                    val p:Address = pend._1
                    val message:DataMessage = pend._2
                    //println(self + ": p=" + p + " message= " + message);
                    val (w, payload) = message match {
                        case DataMessage(vc: VectorClock, payl: KompicsEvent) => (vc, payl)
                    };
                    //println(self + ": w=" + w + " vec=" + vec)
                    if (w<=vec) {
                        pending -= pend;
                        vec.inc(p);
                        trigger(CRB_Deliver(p, payload) -> crb);
                        //println(self + ": delivered " + payload);
                        change = true;
                    }
                ()
            };
        }
        
        
    }
  }
}