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
package se.kth.id2203.kvstore;

import java.util.UUID

import se.kth.id2203.detectors.{BLE_Leader, BallotLeaderElection}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.sl._
import se.sics.kompics.network.Network

import scala.collection.mutable;

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val route = requires(Routing);
  val sc = requires[SequenceConsensus]
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val store = mutable.HashMap.empty[String, String]
  var leader: Option[NetAddress] = None;
  fillStoreInitial(cfg.getValue[Int]("id2203.project.prefillStore"))
  //******* Handlers ******


  net uponEvent {
    case NetMessage(header, op: Op) => handle {
      log.info("Got operation {}!", op);
      trigger(SC_Propose( op ) -> sc)
    }
  }

  sc uponEvent {
    case SC_Decide(operation: Op) => handle {
      log.info(s"Operation $operation decided")
      operation match {
        case Op(_, c: Get, src,  _ )=>  {

          val result = store.get(c.key)
          if(result.isDefined)
            trigger(NetMessage(self, src, operation.response(OpCode.Ok, result)) -> net);
          else
            trigger(NetMessage(self, src, operation.response(OpCode.NotFound)) -> net);
        }
        case Op(_, c: Put, src, _ )=> {
          store += ( c.key -> c.value )
            trigger(NetMessage(self, src, operation.response(OpCode.Ok)) -> net);
        }
        case Op(_, c: Cas, src, _ )=> {
          if (store.contains(c.key) ) {
            val result = store(c. key)
            if ( c.oldValue == result ) {
              store += (c.key -> c.newValue)
                trigger(NetMessage(self, src, operation.response(OpCode.Ok, Some(result))) -> net);
            } else
              trigger(NetMessage(self, src, operation.response(OpCode.ReferenceValuesIsNotCurrentValue)) -> net);
          } else  {
              trigger(NetMessage(self, src, operation.response(OpCode.NotFound)) -> net);
          }
        }
      }

    }
  }

  def fillStoreInitial(amount: Int): Unit = {
    store += ( ("one", "value1") )
    store += ( ("two", "value2") )
    store += ( ("three", "value3") )
    store += ( ("four", "value4") )
    store += ( ("five", "value5") )
    store += ( ("six", "value6") )
    store += ( ("seven", "value7") )
    store += ( ("eight", "value8") )
    store += ( ("nine", "value9") )
    store += ( ("ten", "value10") )
    for (i <- 0.to(amount) ) {
      store += ( ("test"+i, "value" + i) )
    }
  }
}
