package se.kth.id2203.messaging

import se.kth.id2203.networking.NetAddress
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Address
import se.sics.kompics.sl.{ComponentDefinition, Init, Port, handle}

import scala.collection.immutable.Set

case class BEB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

class BestEffortBroadcast extends Port {
  indication[BEB_Deliver];
  request[BEB_Broadcast];
  request[Set_Topology];
}


class BasicBroadcast extends ComponentDefinition {

  //subscriptions
  val pLink = requires(PerfectLink);
  val beb = provides[BestEffortBroadcast];

  //configuration
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var topology = Set[NetAddress]();

  //handlers
  beb uponEvent {
    case x: BEB_Broadcast => handle {
      //println( s"sending broadcast with $topology")
      for (q <- topology) {
        trigger(PL_Send(q, x) -> pLink)
      }

    }
    case Set_Topology(setNodes) => handle {
      topology = setNodes
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, BEB_Broadcast(payload)) => handle {
      trigger(BEB_Deliver(src, payload) -> beb)

    }
  }
}
