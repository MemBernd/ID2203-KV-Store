package se.kth.id2203.detectors
import se.kth.id2203.messaging._
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start, ComponentDefinition => _, Port => _}



class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect];
  indication[Restore];
  request[Monitor]
}

case class Suspect(process: NetAddress) extends KompicsEvent;
case class Restore(process: NetAddress) extends KompicsEvent;


case class HeartbeatReply(seq: Int) extends KompicsEvent;
case class HeartbeatRequest(seq: Int) extends KompicsEvent;

//Define EPFD Implementation
class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {

  //EPFD subscriptions
  val timer = requires[Timer];
  val pLink = requires(PerfectLink);
  val epfd = provides[EventuallyPerfectFailureDetector];

  // EPDF component state and initialization

  //configuration parameters
  val self  = epfdInit match {
    case Init( s: NetAddress) => s
  };
  var topology = List[NetAddress]()
  val delta = cfg.getValue[Long]("id2203.project.keepAlivePeriod");

  //mutable state
  var period = delta
  var alive = Set(topology: _*);
  var suspected = Set[NetAddress]();
  var seqnum = 0;

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  //EPFD event handlers
  ctrl uponEvent {
    case _: Start => handle {
    }
  }

  epfd uponEvent {
    case Monitor(nodes) => handle {
      startTimer(period)
      topology = nodes
      alive = Set(topology: _*)
      suspected = Set[NetAddress]()
      seqnum = 0;
      //println(s"topology $topology")
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => handle {
      if (!alive.intersect(suspected).isEmpty) {
        period += delta

      }

      seqnum = seqnum + 1;

      for (p <- topology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          log.debug(s"$self suspects $p")
          suspected = suspected + p
          trigger(Suspect(p) -> epfd)
          println(s"EvP. suspecting $p")
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          println(s"EvP. not suspecting $p")
          trigger(Restore(p) -> epfd);
        }
        trigger(PL_Send(p, HeartbeatRequest(seqnum)) -> pLink);
      }
      alive = Set[NetAddress]();
      startTimer(period);
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, HeartbeatRequest(seq)) => handle {

      trigger(PL_Send(src, HeartbeatReply(seq)) -> pLink);

    }
    case PL_Deliver(src, HeartbeatReply(seq)) => handle {

      if (seq == seqnum || suspected.contains(src)) alive = alive + src

    }
  }
};
