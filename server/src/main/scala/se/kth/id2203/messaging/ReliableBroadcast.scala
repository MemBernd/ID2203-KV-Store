package se.kth.id2203.messaging

import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Address
import se.sics.kompics.sl.{ComponentDefinition, Init, Port, handle}

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
