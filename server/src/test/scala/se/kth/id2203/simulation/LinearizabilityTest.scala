package se.kth.id2203.simulation

import org.scalatest._
import se.kth.id2203.ParentComponent
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import java.net.{InetAddress, UnknownHostException}

import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.result.SimulationResultSingleton

import scala.concurrent.duration._


class LinearizabilityTest extends FlatSpec with Matchers {

  private val nMessages = 1;

  //  "Classloader" should "be something" in {
  //    val cname = classOf[SimulationResultSingleton].getCanonicalName();
  //    var cl = classOf[SimulationResultSingleton].getClassLoader;
  //    var i = 0;
  //    while (cl != null) {
  //      val res = try {
  //        val c = cl.loadClass(cname);
  //        true
  //      } catch {
  //        case t: Throwable => false
  //      }
  //      println(s"$i -> ${cl.getClass.getName} has class? $res");
  //      cl = cl.getParent();
  //      i -= 1;
  //    }
  //  }

  "LIN o(w) < o(w) " should "succed" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val key = LinearizabilityScenario.keyOwOw
    val OwOw = LinearizabilityScenario.scenarioOwOw();
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    OwOw.simulate(classOf[LauncherComp])
    SimulationResult.get[String](key) should be (Some("Ok"));
  }

  "LIN o(r) < o(w) " should "succed" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val key = LinearizabilityScenario.keyOrOw
    val value = LinearizabilityScenario.valueOrOw
    val OrOw = LinearizabilityScenario.scenarioOrOw();
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    OrOw.simulate(classOf[LauncherComp])
    SimulationResult.get[String](key) should be (Some("Ok"));
  }

  "LIN o(w) < o(r) " should "succed" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val key = LinearizabilityScenario.keyOwOr
    val value = LinearizabilityScenario.valueOwOr
    val OrOw = LinearizabilityScenario.scenarioOwOr();
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    OrOw.simulate(classOf[LauncherComp])
    SimulationResult.get[String](key) should be (Some("Ok"));
  }

}

object LinearizabilityScenario {

  def partitions = 1
  def replicationDegree = 3;
  import Distributions._
  var keyOwOw: String = "keyOwOw"
  var valueOwOw: String = "valueOwOw"
  var keyOwOr: String = "keyOwOr"
  var valueOwOr: String = "valueOwOr"
  var keyOrOw:String = "keyOrOw"
  var valueOrOw:String = "valueOrOw"
  var keyOrOr: String = "keyOrOr"
  var valueOrOr = "valueOrOr"
  var key = "key"
  var v = "value"

  // needed for the distributions, but needs to be initialised after setting the seed
  implicit val random = JSimulationScenario.getRandom();

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1;


  val startServerOp = Op { (self: Integer) =>


    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr,
        "id2203.project.partitions" -> partitions,
        "id2203.project.replicationDegree" -> replicationDegree,
        "id2203.project.prefillStore" -> 100)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1),
        "id2203.project.partitions" -> partitions,
        "id2203.project.replicationDegree" -> replicationDegree,
        "id2203.project.prefillStore" -> 100)
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  };


  val startClientPut1 = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    println(" hello put" + key + " " + v)
    StartNode(selfAddr, Init[PutClient](keyOwOw, valueOwOw, 1), conf);
  };

  val startClientCasAfterPut = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    println(" hello " + key + " " + v)
    StartNode(selfAddr, Init[CasScenarioClient](keyOwOw, valueOwOw, "newValue", 1), conf);
  };

  def scenarioOwOw(): JSimulationScenario = {
    val startCluster = raise(replicationDegree, startServerOp, 1.toN).arrival(constant(2.second));
    val startClientP = raise(1, startClientPut1, 1.toN).arrival(constant(1.second));
    val startClientCas = raise(2, startClientCasAfterPut, 2.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClientP andThen
      60.seconds afterTermination startClientCas andThen
      60.seconds afterTermination Terminate
  }

  val startClientGet1 = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[GetReturnScenarioClient](keyOrOw, true), conf);
  };



  val startClientCasAfterGet = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[CasScenarioClient](keyOrOw, null, v, 1), conf);
  };



  def scenarioOrOw(): JSimulationScenario = {

    val startCluster = raise(replicationDegree, startServerOp, 1.toN).arrival(constant(2.second));
    val startClientGet = raise(1, startClientGet1, 1.toN).arrival(constant(1.second));
    val startClientCas = raise(2, startClientCasAfterGet, 2.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClientGet andThen
      60.seconds afterTermination startClientCas andThen
      60.seconds afterTermination Terminate
  }

  val startClientPut2 = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    println(" hello put" + key + " " + v)
    StartNode(selfAddr, Init[PutClient](keyOwOr, valueOwOr, 1), conf);
  };

  val startClientGet2 = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[GetReturnScenarioClient](keyOwOr, false), conf);
  };

  def scenarioOwOr(): JSimulationScenario = {

    val startCluster = raise(replicationDegree, startServerOp, 1.toN).arrival(constant(2.second));
    val startClientPut = raise(1, startClientPut2, 1.toN).arrival(constant(1.second));
    val startClientGet = raise(2, startClientGet2, 2.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClientPut andThen
      60.seconds afterTermination startClientGet andThen
      60.seconds afterTermination Terminate
  }


}