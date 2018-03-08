package se.kth.edx.id2203.core

import java.net.{ InetAddress, InetSocketAddress }

import se.kth.edx.id2203.core.ExercisePrimitives.PerfectP2PLink._
import se.kth.edx.id2203.core.Ports._
import se.sics.kompics.{ Init, KompicsEvent }
import se.sics.kompics.network.{ Address, Network, Transport }
import se.sics.kompics.sl.{ ComponentDefinition, _ }

object ExercisePrimitives {

    object AddressUtils {

        def toAddress(x: Int): Address = {
            TAddress(new InetSocketAddress(InetAddress.getByName("192.168.2." + x.toString), 1000))
        }

        def toRank(addr: Address): Int = {
            return addr.getIp().getAddress()(3).toInt;
        }

    }

    object PerfectP2PLink {
        case class PerfectLinkInit(selfAddr: Address) extends Init[PerfectP2PLink];
        case class PerfectLinkMessage(src: Address, dest: Address, payload: KompicsEvent) extends TMessage(THeader(src, dest, Transport.TCP));
    }

    class PerfectP2PLink(pp2pInit: PerfectLinkInit) extends ComponentDefinition {

        val pLink = provides[PerfectLink];
        val network = requires[Network];

        val self = pp2pInit.selfAddr;

        pLink uponEvent {
            case PL_Send(dest, payload) => handle {
                trigger(PerfectLinkMessage(self, dest, payload) -> network);
            }
        }

        network uponEvent {
            case PerfectLinkMessage(src, dest, payload) => handle {
                trigger(PL_Deliver(src, payload) -> pLink);
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

}