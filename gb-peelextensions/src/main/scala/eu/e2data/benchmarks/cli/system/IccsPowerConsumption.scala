package eu.e2data.benchmarks.cli.system

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._

class IccsPowerConsumption (
                             version      : String,
                             configKey    : String,
                             lifespan     : Lifespan,
                             dependencies : Set[System] = Set(),
                             mc           : Mustache.Compiler) extends System("pdu", version, configKey, lifespan, dependencies, mc) {

  def enabled = config.getBoolean("system.pdu.enabled")

  var pid = -1;

  override def configuration() = SystemConfig(config, {
//    val conf = config.getString(s"system.$configKey.path.config")
    List(
//      SystemConfig.Entry[Model.Yaml](s"system.$configKey", s"$conf/application.conf", templatePath("conf/application.conf"), mc)
    )
  })


  override def beforeRun(run: Experiment.Run[System]): Unit = {
    if (enabled) {
      val masters = config.getStringList("system.pdu.config.masters").asScala.toSet
      val slaves = config.getStringList("system.pdu.config.slaves").asScala.toSet
      val hosts = masters.++(slaves).asJavaCollection

      val utilsPath = config.getString("app.path.utils")
      val logDir = Paths.get(run.home, "logs", name)
      if (!Files.exists(logDir)) {
        Files.createDirectories(logDir)
        logger.info(s"Ensuring log folder '$logDir' exists")
      }
      val nodes = String.join(" ", hosts)
      pid = (shell ! s"""  nohup python $utilsPath/pyscripts/powerScript.py $logDir/pdu.log $nodes >/dev/null 2>/dev/null & echo $$!' """)
      logger.info("Start PDU monitoring for nodes: {}", nodes)
    }
  }

  override def afterRun(run: Experiment.Run[System]): Unit = {
    if (enabled) {
      shell ! s"kill ${pid}"
      logger.info(s"Stop PDU monitoring process with PID: ${pid}'")
    }
  }

  override def start(): Unit = {
    logger.info("Start System APC PDU monitoring");
  }

  override def stop(): Unit = {
    logger.info("Stop System APC PDU monitoring");
  }

  override def isRunning = {
    (shell ! s"""ps -p `cat ${config.getString("system.flink.config.yaml.env.pid.dir")}/flink-*.pid`""") == 0
  }

}
