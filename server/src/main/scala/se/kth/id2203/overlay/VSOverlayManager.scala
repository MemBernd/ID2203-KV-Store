/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.overlay;

import se.kth.id2203.bootstrapping._
import se.kth.id2203.networking._
import se.sics.kompics.sl._
import se.sics.kompics.network.Network
import se.sics.kompics.timer.Timer
import se.kth.id2203.detectors._
import se.kth.id2203.kvstore.SequenceConsensus
import se.kth.id2203.messaging._
import se.kth.id2203.messaging.PerfectP2PLink.PerfectLinkInit

import scala.util.Random;

/**
 * The V(ery)S(imple)OverlayManager.
 * <p>
 * Keeps all nodes in a single partition in one replication group.
 * <p>
 * Note: This implementation does not fulfill the project task. You have to
 * support multiple partitions!
 * <p>
 * @author Lars Kroll <lkroll@kth.se>
 */
class VSOverlayManager extends ComponentDefinition {

  //******* Ports ******
  val route = provides(Routing);
  val boot = requires(Bootstrapping);
  val net = requires[Network];
  val timer = requires[Timer];
  val evP = requires[EventuallyPerfectFailureDetector]
  val beb = requires[BestEffortBroadcast]
  val sc = requires[SequenceConsensus]
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val replicationDegree = cfg.getValue[Int]("id2203.project.replicationDegree");

  val partitions = cfg.getValue[Int]("id2203.project.partitions")
  var partition = 0
  var lut: Option[LookupTable] = None
  var replicationGroup: Set[NetAddress] = null
  var suspected = Set[NetAddress]();

  //******* Handlers ******

  beb uponEvent {
    case BEB_Deliver(src, payload) => handle {
      log.info( s"received broadcast from $src with $payload")
    }
  }

  boot uponEvent {
    case GetInitialAssignments(nodes) => handle {
      log.info("Generating LookupTable...");
      val lut = LookupTable.generate(nodes, partitions, replicationDegree);
      logger.debug("Generated assignments:\n");
      trigger (new InitialAssignments(lut) -> boot);
    }
    case Booted(assignment: LookupTable) => handle {
      log.info("Got NodeAssignment, overlay ready:" + assignment);
      lut = Some(assignment);

      partition = assignment.getPartition(self)
      assert(partition != 0)
      replicationGroup = assignment.getNodes(partition)
      log.debug(s"$self: partition $partition replication group consist of: $replicationGroup")

      //setup Beb for transmitting to all
      trigger( Set_Topology(assignment.getNodes()) -> beb )
      trigger( BEB_Broadcast( TEST(self + "said hi") ) -> beb )

      //start Ev.P for all nodes in the LookupTable
      trigger( Monitor(assignment.getNodes().toList) -> evP )

      //setup and start sequence consensus
      trigger( Set_Topology(replicationGroup) -> sc)

    }
  }

  evP uponEvent {
    case Suspect(p) => handle {
      suspected += p
    }
    case Restore(p) => handle {
      suspected -= p
    }
  }

  net uponEvent {
    case NetMessage(header, RouteMsg(key, msg)) => handle {
      val target = routingTarget(key)
      log.info(s"Forwarding message for key $key to $target");
      trigger(NetMessage(header.src, target, msg) -> net);
    }
    case NetMessage(header, msg: Connect) => handle {
      lut match {
        case Some(l) => {
          log.debug(s"Accepting connection request from ${header.src}");
          val nodes = l.getNodes().toList
          trigger (NetMessage(self, header.src, msg.ack(nodes) ) -> net);
        }
        case None => log.info("Rejecting connection request from ${header.src}, as system is not ready, yet.");
      }
    }
  }

  route uponEvent {
    case RouteMsg(key, msg) => handle {
      val target = routingTarget(key)
      log.info(s"Routing message for key $key to $target");
      trigger (NetMessage(self, target, msg) -> net);
    }
  }

  def routingTarget(key: String): NetAddress = {
    val nodes = lut.get.lookup(key).toSet.diff(suspected);
    //log.debug(s"nodes to route to: $nodes")
    assert(!nodes.isEmpty);
    if (nodes.contains(self))
      return self
    val i = Random.nextInt(nodes.size);
    return nodes.drop(i).head;
  }
}
