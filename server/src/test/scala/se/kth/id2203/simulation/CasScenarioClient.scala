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
package se.kth.id2203.simulation

import java.util.UUID

import se.kth.id2203.kvstore._
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer

import scala.collection.mutable;

class CasScenarioClient(init: Init[CasScenarioClient]) extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val timer = requires[Timer];
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address");
  private val pending = mutable.Map.empty[UUID, String];
  private val values = mutable.Map.empty[UUID, Operation]
  //******* Handlers ******
  ctrl uponEvent {
    case _: Start => handle {
      val (prefix:String, prefixValue:String, prefixNewValue:String, messages:Int) = init match {
        case Init(prefix: String, prefixValue: String, prefixNew:String, m: Int) => (prefix, prefixValue, prefixNew, m)
        case Init(prefix: String, prefixValue: String, m: Int) => (prefix, prefixValue, "new", m)
        case Init(prefix: String, prefixValue: String) => (prefix, prefixValue, "new", SimulationResult[Int]("messages"))
        case _ => ("test", "value", SimulationResult[Int]("messages"))
      }
      if (messages == 1 && prefixValue != null)  {
        val oldValue = SimulationResult.get[String](prefix)
        if (oldValue.isDefined)
          sendOp(prefix, oldValue.get, prefixNewValue)
        else
          SimulationResult += (prefix -> "key in SimulationResult expected, but not found")
      } else if (messages == 1) {
        sendOp(prefix, prefixValue, prefixNewValue)
      } else if (messages > 1) {
        for (i <- 0 to messages) {
          val key = prefix +""+ i
          val value = prefixValue +"" +i
          sendOp(key, value, prefixNewValue+""+i)
        }
      }

    }
  }

  net uponEvent {
    case NetMessage(header, or @ OpResponse(id, status, command, value)) => handle {
      logger.debug(s"Got OpResponse: $or");
      pending.remove(id) match {
        case Some(key) => SimulationResult += (key -> status.toString());
        case None      => logger.warn(s"ID $id was not pending! Ignoring response.");
      }
    }

  }

  def sendOp(key:String, oldValue: String, newValue:String): Unit = {
    val op = new Op(key, Cas(key, oldValue, newValue), self);
    val routeMsg = RouteMsg(op.key, op);
    println(" hello " + op)
    trigger(NetMessage(self, server, routeMsg) -> net);
    pending += (op.id -> op.key);
    logger.info("Sending {}", op);
    SimulationResult += (op.key -> "Sent");
  }
}
