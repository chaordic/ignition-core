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
                          extraArgs: Map[String, String] = Map.empty)

  def mountStorage(containerName: String)(implicit mounts: Seq[String]): Unit = {
    if (!mounts.contains(s"/mnt/$containerName")) {
      println(s"Mounting s3a://$containerName as /mnt/$containerName")
      dbutils.fs.mount(s"s3a://$containerName", s"/mnt/$containerName")
    }
  }

  def mountBlobStorage(containerName: String)(implicit mounts: Seq[String]): Unit = {
    if (!mounts.contains(s"/mnt/$containerName")) {
      val sas = "?sv=2020-04-08&ss=bf&srt=sco&st=2021-06-09T14%3A25%3A01Z&se=2025-12-31T14%3A25%3A00Z&sp=rwdlcu&sig=aAeOjf89E09SBs88%2FCeX8rZk4BmyRhIx51rCSs2Hhok%3D" // sys.env.getOrElse("AZ_BLOB_STORAGE_SAS", "")

      val storageAccountName = "mailncdevqryv"

      val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

      val source = s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net"

      println(s"Mounting $source as /mnt/$containerName")
      dbutils.fs.mount(
        source = source,
        mountPoint = s"/mnt/$containerName",
        extraConfigs = Map(config -> sas)
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
        c.copy(date = new DateTime(x))
      }
      opt[String]('t', "runner-tag") action { (x, c) =>
        c.copy(tag = DateTime.now().toString().substring(0, 19).replaceAll(":", "_").replaceAll("-", "_") + "UTC")
        // c.copy(tag = x)
      }
      opt[String]('u', "runner-user") action { (x, c) =>
        c.copy(user = "claudio-bordoni")
        // c.copy(user = x)
      }
      opt[String]('m', "runner-master") action { (x, c) =>
        c.copy(master = x)
      }
      opt[String]('e', "runner-executor-memory") action { (x, c) =>
        c.copy(executorMemory = x)
      }

      opt[(String, String)]('w', "runner-extra") unbounded() action { (x, c) =>
        c.copy(extraArgs = c.extraArgs ++ Map(x))
      }
    }

    parser.parse(args, RunnerConfig()) map { config =>
      val setup = jobsSetups.get(config.setupName)

      println(s"${config.setupName} v1.1.30")
      println(s"Running with extra ${config.extraArgs}")
      println(s"Running with tag ${config.tag}")

      require(setup.isDefined,
        s"Invalid job setup ${config.setupName}, available jobs setups: ${jobsSetups.keySet}")

      val Some((jobSetup, jobConf)) = setup

      val appName = s"${config.setupName}.${config.tag}"

      val builder = SparkSession.builder

      builder.config("spark.executor.memory", config.executorMemory)

      builder.config("spark.eventLog.dir", "file:///media/tmp/spark-events")

      /*
      try {
        dbutils.fs.unmount(s"/mnt/mail-ignition")
        dbutils.fs.unmount(s"/mnt/chaordic-engine")
        dbutils.fs.unmount(s"/mnt/chaordic-dumps")
        dbutils.fs.unmount(s"/mnt/platform-dumps-virginia")
        dbutils.fs.unmount(s"/mnt/azure-bs-teste")
      } catch {
        case _: Throwable => println("Got some other kind of Throwable exception")
      }
      */

      implicit val mounts: Seq[String] = dbutils.fs.mounts().map(_.mountPoint)

      List("mail-ignition", "chaordic-engine", "chaordic-dumps", "platform-dumps-virginia").foreach(mountStorage)
      List("azure-bs-teste").foreach(mountBlobStorage)

      builder.master(config.master)
      builder.appName(appName)

      builder.config("spark.hadoop.mapred.output.committer.class", classOf[DirectOutputCommitter].getName())

      defaultSparkConfMap.foreach { case (k, v) => builder.config(k, v) }

      jobConf.foreach { case (k, v) => builder.config(k, v) }

      // Add logging context to driver
      setLoggingContextValues(config)

      try {
        builder.enableHiveSupport()
      } catch {
        case t: Throwable => logger.warn("Failed to enable HIVE support", t)
      }

      val session = builder.getOrCreate()

      val sc = session.sparkContext

      // Also try to propagate logging context to workers
      // TODO: find a more efficient and bullet-proof way
      val configBroadCast = sc.broadcast(config)

      sc.parallelize(Range(1, 2000), numSlices = 2000).foreachPartition(_ => setLoggingContextValues(configBroadCast.value))

      val context = RunnerContext(sc, session, config)

      try {
        jobSetup.apply(context)
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          System.exit(1) // force exit of all threads
      }

      import scala.concurrent.ExecutionContext.Implicits.global
      Future {
        // If everything is fine, the system will shut down without the help of this thread and YARN will report success
        // But sometimes it gets stuck, then it's necessary to use the force, but this may finish the job as failed on YARN
        Thread.sleep(30 * 1000)
        System.exit(0) // force exit of all threads
      }
    }
  }
}
