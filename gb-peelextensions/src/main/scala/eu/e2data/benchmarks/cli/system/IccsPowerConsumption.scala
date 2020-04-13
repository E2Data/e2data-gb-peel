package eu.e2data.benchmarks.cli.system

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.{SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._

class IccsPowerConsumption (
                             version      : String,
                             configKey    : String,
                             lifespan     : Lifespan,
                             dependencies : Set[System] = Set(),
                             mc           : Mustache.Compiler) extends System("pdu", version, configKey, lifespan, dependencies, mc) {

  var pid = "";

  override def configuration() = SystemConfig(config, {
    List()
  })


  override def beforeRun(run: Experiment.Run[System]): Unit = {
    val master = config.getString("system.default.config.yaml.jobmanager.rpc.address")
    val slaves = config.getStringList("system.default.config.slaves").asScala.toSet
    val hosts = master + slaves

    val utilsPath = config.getString("app.path.utils")
    val logDir = Paths.get(run.home, "logs", name)
    if (!Files.exists(logDir)) {
      Files.createDirectories(logDir)
      logger.info(s"Ensuring dstat results folder '$logDir' exists")
    }
    val nodes = String.join(",", hosts)
    pid = (shell !! s"""  nohup python $utilsPath/pyscripts/powerScript.py $logDir/pdu.log $nodes >/dev/null 2>/dev/null & echo $$!' """).trim
    logger.info("Start measuring Power Consumption for nodes {}", nodes)
  }

  override def afterRun(run: Experiment.Run[System]): Unit = {
    shell ! s"kill ${pid}"
    logger.info(s"Power Consumption with PID ${pid} stopped'")
  }

  override def start(): Unit = {
    logger.info("Start APC PDU  monitoring");
  }

  override def stop(): Unit = {
    logger.info("Stop APC PDU  monitoring");
  }

  override def isRunning = {
    (shell ! s"""ps -p `cat ${config.getString("system.flink.config.yaml.env.pid.dir")}/flink-*.pid`""") == 0
  }

}
