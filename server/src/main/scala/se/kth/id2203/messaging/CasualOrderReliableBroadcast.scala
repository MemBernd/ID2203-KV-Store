package se.kth.id2203.messaging

import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.{ComponentDefinition => _, Port => _, KompicsEvent}

import scala.collection.immutable.Set
import scala.collection.mutable.ListBuffer


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


case class VectorClock(var vc: Map[Address, Int]) {

  def inc(addr: Address) = {
    vc = vc + ((addr, vc.get(addr).get + 1));
  }

  def set(addr: Address, value: Int) = {
    vc = vc + ((addr, value));
  }

  def <=(that: VectorClock): Boolean = vc.foldLeft[Boolean](true)((leq, entry) => leq & (entry._2 <= that.vc.getOrElse(entry._1, entry._2)))

}

object VectorClock {

  def empty(topology: scala.Seq[Address]): VectorClock = {
    VectorClock(topology.foldLeft[Map[Address, Int]](Map[Address, Int]())((mp, addr) => mp + ((addr, 0))))
  }

  def apply(that: VectorClock): VectorClock = {
    VectorClock(that.vc);
  }

}