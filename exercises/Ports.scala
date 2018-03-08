package se.kth.edx.id2203.core

import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Address
import se.sics.kompics.sl._

object Ports {

  case class PL_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
  case class PL_Send(dest: Address, payload: KompicsEvent) extends KompicsEvent;

  class PerfectLink extends Port {
    indication[PL_Deliver];
    request[PL_Send];
  }

  case class Suspect(src: Address) extends KompicsEvent;
  case class Restore(src: Address) extends KompicsEvent;

  class EventuallyPerfectFailureDetector extends Port {
    indication[Suspect];
    indication[Restore];
  }

  case class BEB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
  case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

  class BestEffortBroadcast extends Port {
    indication[BEB_Deliver];
    request[BEB_Broadcast];
  }

  case class RB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
  case class RB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

  class ReliableBroadcast extends Port {
    indication[RB_Deliver];
    request[RB_Broadcast];
  }

  case class CRB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
  case class CRB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

  class CausalOrderReliableBroadcast extends Port {
    indication[CRB_Deliver];
    request[CRB_Broadcast];
  }
  
  
  case class AR_Read_Request() extends KompicsEvent
  case class AR_Read_Response(value: Option[Any]) extends KompicsEvent
  case class AR_Write_Request(value: Any) extends KompicsEvent
  case class AR_Write_Response() extends KompicsEvent
  
  class AtomicRegister extends Port {
    request[AR_Read_Request]
    request[AR_Write_Request]
    indication[AR_Read_Response]
    indication[AR_Write_Response]
  }
  
  case class C_Decide(value: Any) extends KompicsEvent;
  case class C_Propose(value: Any) extends KompicsEvent;
  
  class Consensus extends Port{
    request[C_Propose];
    indication[C_Decide];
  }
  

}