package se.kth.id2203.messaging

import java.net.{InetAddress, InetSocketAddress}

import se.kth.id2203.networking.NetAddress
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.Address
import se.sics.kompics.timer.{ScheduleTimeout, Timeout}

case class Set_Topology(topology: Set[NetAddress]) extends KompicsEvent
case class Monitor(nodes: List[NetAddress]) extends KompicsEvent
case class TEST(content: String) extends KompicsEvent;
case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);


object AddressUtils {

  def toAddress(x: Int): NetAddress = {
    NetAddress(new InetSocketAddress(InetAddress.getByName("192.168.2." + x.toString), 1000))
  }

  def toRank(addr: Address): Int = {
    return addr.getIp().getAddress()(3).toInt;
  }

}
