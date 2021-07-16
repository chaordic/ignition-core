package ignition.core.jobs

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object CoreJobRunner {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  case class RunnerContext(sparkContext: SparkContext,
                           sparkSession: SparkSession,
                           config: RunnerConfig)


  // Used to provide contextual logging
  def setLoggingContextValues(config: RunnerConfig): Unit = {
    try { // yes, this may fail but we don't want everything to shut down
      org.slf4j.MDC.put("setupName", config.setupName)
      org.slf4j.MDC.put("tag", config.tag)
      org.slf4j.MDC.put("user", config.user)
    } catch {
      case e: Throwable =>
        // cry
    }
  }

  case class RunnerConfig(setupName: String = "nosetup",
                          date: DateTime = DateTime.now.withZone(DateTimeZone.UTC),
                          tag: String = DateTime.now.withZone(DateTimeZone.UTC).toString().substring(0, 19).replaceAll(":", "_").replaceAll("-", "_") + "UTC",
                          user: String = "nouser",
                          master: String = "local[*]",
                          executorMemory: String = "2G",
                          extraArgs: Map[String, String] = Map.empty,
                          apiKeys: Set[String] = Set.empty)

  def mountStorage(containerName: String, mountPoint: String)(implicit mounts: Seq[String]): Unit = {
    if (!mounts.contains(s"/mnt/$mountPoint")) {
      dbutils.fs.mount(s"s3a://$containerName", s"/mnt/$mountPoint")
    }
  }

  def mountBlobStorage(containerName: String, sas: String)(implicit mounts: Seq[String]): Unit = {
    val sasValue = sys.env.get(sas)
    
    if (sasValue.isEmpty) {
      throw new Exception(s"No value provided for ${sas}. Verify your cluster env settings")
    }
    
    if (!mounts.contains(s"/mnt/$containerName")) {
      val storageAccountName = sys.env.getOrElse("STORAGE_ACCOUNT_NAME", "")

      val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

      val source = s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net"

      dbutils.fs.mount(
        source = source,
        mountPoint = s"/mnt/$containerName",
        extraConfigs = Map(config -> sasValue.get)
      )
    }
  }

  def runJobSetup(args: Array[String], jobsSetups: Map[String, (CoreJobRunner.RunnerContext => Unit, Map[String, String])], defaultSparkConfMap: Map[String, String]) {
    val parser = new scopt.OptionParser[RunnerConfig]("Runner") {
      help("help") text("prints this usage text")
      
      arg[String]("<setup-name>") required() action { (x, c) =>
        c.copy(setupName = x)
      } text(s"one of ${jobsSetups.keySet}")
      // Note: we use runner-option name because when passing args to spark-submit we need to avoid name conflicts
      opt[String]('d', "runner-date") action { (x, c) =>
        c.copy(date = new DateTime(x).withZone(DateTimeZone.UTC))
      }
      opt[String]('t', "runner-tag") action { (x, c) =>
        c.copy(tag = x)
      }
      opt[String]('u', "runner-user") action { (x, c) =>
        c.copy(user = x)
      }
      opt[String]('m', "runner-master") action { (x, c) =>
        c.copy(master = x)
      }
      opt[String]('e', "runner-executor-memory") action { (x, c) =>
        c.copy(executorMemory = x)
      }
      opt[String]('a', "runner-apikeys") action { (x, c) =>
        c.copy(apiKeys = if (x != "*") x.split(",").toSet else Set.empty)
      }
      opt[(String, String)]('w', "runner-extra") unbounded() action { (x, c) =>
        c.copy(extraArgs = c.extraArgs ++ Map(x))
      }
    }

    parser.parse(args, RunnerConfig()) map { config =>
      val setup = jobsSetups.get(config.setupName)

      require(setup.isDefined,
        s"Invalid job setup ${config.setupName}, available jobs setups: ${jobsSetups.keySet}")

      println(s"Running ${config.setupName} with user ${config.user}")
      println(s"Running with tag ${config.tag}")

      if (config.apiKeys.nonEmpty) {
        println(s"Running for ${config.apiKeys.mkString(", ")}")
      }

      val Some((jobSetup, jobConf)) = setup

      val appName = s"${config.setupName}.${config.tag}"

      val builder = SparkSession.builder

      builder.config("spark.executor.memory", config.executorMemory)

      builder.config("spark.eventLog.dir", "file:///media/tmp/spark-events")

      implicit val mounts: Seq[String] = dbutils.fs.mounts().map(_.mountPoint)

      mountStorage("chaordic-engine", "chaordic-engine")
      mountStorage("mail-ignition", "aws-mail-ignition")

      mountBlobStorage("mail-ignition", "MAIL_IGNITION_SAS")
      mountBlobStorage("platform-dumps-virginia", "PLATFORM_DUMPS_SAS")
      mountBlobStorage("chaordic-dumps", "CHAORDIC_DUMPS_SAS")

      builder.master(config.master)
      builder.appName(appName)

      defaultSparkConfMap.foreach { case (k, v) => builder.config(k, v) }

      jobConf.foreach { case (k, v) => builder.config(k, v) }

      // Add logging context to driver
      setLoggingContextValues(config)

      val session = builder.getOrCreate()

      val sc = session.sparkContext

      // Also try to propagate logging context to workers
      // TODO: find a more efficient and bullet-proof way
      val configBroadCast = sc.broadcast(config)

      sc.parallelize(Range(1, 2000), numSlices = 2000).foreachPartition(_ => setLoggingContextValues(configBroadCast.value))

      val context = RunnerContext(sc, session, config)

      jobSetup.apply(context)
    }
  }
}
