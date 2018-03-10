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
  val store = mutable.HashMap.empty[String, Int]
  fillStoreInitial(cfg.getValue[Int]("id2203.project.prefillStore"))
  //******* Handlers ******
  net uponEvent {
    case NetMessage(header, op: Op) => handle {
      log.info("Got operation {}! Now implement me please :)", op);
      val result = store.get(op.key)
      if(result.isDefined)
        trigger(NetMessage(self, header.src, op.response(OpCode.Ok)) -> net);
      else
        trigger(NetMessage(self, header.src, op.response(OpCode.NotFound)) -> net);

    }
  }

  sc uponEvent {
    case SC_Decide(value) => handle {
      println(value + " was decided")
    }
  }

  def fillStoreInitial(amount: Int): Unit = {
    store += ( ("one", 1) )
    store += ( ("two", 2) )
    store += ( ("three", 3) )
    store += ( ("four", 4) )
    store += ( ("five", 5) )
    store += ( ("six", 6) )
    store += ( ("seven", 7) )
    store += ( ("eight", 8) )
    store += ( ("nine", 9) )
    store += ( ("ten", 10) )
    for (i <- 0.to(amount) ) {
      store += ( ("test"+i, i) )
    }
  }
}
