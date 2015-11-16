package ignition.core.jobs

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTimeZone, DateTime}

import scala.util.Try

object CoreJobRunner {

  case class RunnerContext(sparkContext: SparkContext,
                           config: RunnerConfig)


  // Used to provide contextual logging
  def setLoggingContextValues(config: RunnerConfig): Unit = {
    org.slf4j.MDC.put("setupName", config.setupName)
    org.slf4j.MDC.put("tag", config.tag)
    org.slf4j.MDC.put("user", config.user)
  }

  case class RunnerConfig(setupName: String = "nosetup",
                          date: DateTime = DateTime.now.withZone(DateTimeZone.UTC),
                          tag: String = "notag",
                          user: String = "nouser",
                          master: String = "local[*]",
                          executorMemory: String = "2G",
                          additionalArgs: Map[String, String] = Map.empty)

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

      opt[(String, String)]('w', "runner-with-arg") unbounded() action { (x, c) =>
        c.copy(additionalArgs = c.additionalArgs ++ Map(x))
      }
    }

    parser.parse(args, RunnerConfig()) map { config =>
      val setup = jobsSetups.get(config.setupName)

      require(setup.isDefined,
        s"Invalid job setup ${config.setupName}, available jobs setups: ${jobsSetups.keySet}")

      val Some((jobSetup, jobConf)) = setup

      val appName = s"${config.setupName}.${config.tag}"

      val sparkConf = new SparkConf()
      sparkConf.set("spark.executor.memory", config.executorMemory)

      sparkConf.setMaster(config.master)
      sparkConf.setAppName(appName)
      
      defaultSparkConfMap.foreach { case (k, v) => sparkConf.set(k, v) }

      jobConf.foreach { case (k, v) => sparkConf.set(k, v) }

      // Add logging context to driver
      setLoggingContextValues(config)
      
      val sc = new SparkContext(sparkConf)

      // Also try to propagate logging context to workers
      // TODO: find a more efficient and bullet-proof way
      val configBroadCast = sc.broadcast(config)
      sc.parallelize(Range(1, 2000), numSlices = 2000).foreachPartition(_ => setLoggingContextValues(configBroadCast.value))

      val context = RunnerContext(sc, config)

      try {
        jobSetup.apply(context)
      } catch {
        case t: Throwable =>
          t.printStackTrace()
          System.exit(1) // force exit of all threads
      }
      Try { sc.stop() }
      System.exit(0) // force exit of all threads
    }
  }
}
