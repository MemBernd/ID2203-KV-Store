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


class ResilienceTest extends FlatSpec with Matchers {

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

  "Get w/ 2/3 nodes" should "work after initial leader (out of 3) died" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = GetScenario.scenarioWorking(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some("Ok"));
    }
  }

  "Get w/ 1/3" should "not work after killing 2 out of 3 nodes" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = GetScenario.scenarioFailing(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some("Sent"));
    }
  }



}

object GetScenario {

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

  val killServer = Op { (server: Integer) =>
    println(" killing " + intToServerAddress(server))
    KillNode(intToServerAddress(server))
  }

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

  val startClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(2));
    StartNode(selfAddr, Init.none[ScenarioClient], conf);
  };

  def scenarioWorking(servers: Int): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, startClientOp, 1.toN).arrival(constant(1.second));
    val killBootstrap = raise(1, killServer, 1.toN).arrival(constant(1.second))

    startCluster andThen
      20.seconds afterTermination killBootstrap andThen
      60.seconds afterTermination startClients andThen
      60.seconds afterTermination Terminate
  }

  def scenarioFailing(servers: Int): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, startClientOp, 1.toN).arrival(constant(1.second));
    val killBootstrap = raise(2, killServer, 1.toN).arrival(constant(1.second))

    startCluster andThen
      20.seconds afterTermination killBootstrap andThen
      60.seconds afterTermination startClients andThen
      60.seconds afterTermination Terminate
  }
}