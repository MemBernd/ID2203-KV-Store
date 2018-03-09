package se.kth.id2203.messaging

import se.kth.id2203.networking.NetAddress
import se.sics.kompics.KompicsEvent

case class Set_Topology(topology: Set[NetAddress]) extends KompicsEvent
case class TEST(content: String) extends KompicsEvent;
