package config.fixtures

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** System beans for the bundle. */
@Configuration
class systems extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Systems
  // ---------------------------------------------------

  @Bean(name = Array("flink-1.0.3"))
  def `flink-1.0.3`: Flink = new Flink(
    version      = "1.0.3",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(ctx.getBean("hdfs-2.7.1", classOf[HDFS2])),
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )
}