package se.kth.id2203.simulation

import org.scalatest._
import se.kth.id2203.ParentComponent;
import se.kth.id2203.networking._;
import se.sics.kompics.network.Address
import java.net.{ InetAddress, UnknownHostException };
import se.sics.kompics.sl._;
import se.sics.kompics.sl.simulator._;
import se.sics.kompics.simulator.{ SimulationScenario => JSimulationScenario }
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.result.SimulationResultSingleton;
import scala.concurrent.duration._


class PutCasTest extends FlatSpec with Matchers {

  private val nMessages = 100;

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

  "Cas-NotFound" should "fail by not finding key" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val keyNotFoundCas = PutCasScenario.scenarioCasNotFinding(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    keyNotFoundCas.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"nonexisting$i") should be (Some("NotFound"));
    }
  }

  "Cas-ReferenceValuesIsNotCurrentValue" should "fail by incorrect referenceValue" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val failingGetScenario = PutCasScenario.scenarioCasOldValueIncorrect(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    failingGetScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some("ReferenceValuesIsNotCurrentValue"));
    }
  }

  "Put-Cas sequence" should "work" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val putCasScenario = PutCasScenario.scenarioPutCas(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    putCasScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String]("put"+i) should be (Some("Ok"));
    }
  }

  "Put for several values" should "work" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val putCasScenario = PutCasScenario.scenarioPut(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    putCasScenario.simulate(classOf[LauncherComp]);
    val key = PutCasScenario.key
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](key+i) should be (Some("Ok"));
    }
  }



}

object PutCasScenario {

  import Distributions._

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
        "id2203.project.partitions" -> 1,
        "id2203.project.replicationDegree" -> 3,
        "id2203.project.prefillStore" -> 100)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1),
        "id2203.project.partitions" -> 1,
        "id2203.project.replicationDegree" -> 3,
        "id2203.project.prefillStore" -> 100)
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  };

  val startClientCasNotFinding = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[CasScenarioClient]("nonexisting", "notImportant"), conf);
  };

  def scenarioCasNotFinding(servers: Int): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, startClientCasNotFinding, 1.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClients andThen
      60.seconds afterTermination Terminate
  }

  //incorrect cas value scenario
  def scenarioCasOldValueIncorrect(servers: Int): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, startClientCasOldValueIncorrect, 1.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClients andThen
      60.seconds afterTermination Terminate
  }

  val startClientCasOldValueIncorrect = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[CasScenarioClient]("test", "incorrectOldValue"), conf);
  };



  //put cas combo
  val key = "put"
  val v = "value"
  val startClientPut = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[PutScenarioClient](key, v), conf);
  };

  val startClientCasAfterPut = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init[CasScenarioClient](key, v), conf);
  };


  def scenarioPutCas(servers: Int): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClientsPut = raise(1, startClientPut, 1.toN).arrival(constant(1.second));
    val startClientsCas = raise(1, startClientCasAfterPut, 1.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClientsPut andThen
      60.seconds afterTermination startClientsCas andThen
      60.seconds afterTermination Terminate
  }

  def scenarioPut(servers: Int): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClientsPut = raise(1, startClientPut, 1.toN).arrival(constant(1.second));

    startCluster andThen
      60.seconds afterTermination startClientsPut andThen
      60.seconds afterTermination Terminate
  }
}