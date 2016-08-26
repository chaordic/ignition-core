name := "Ignition-Core"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-Xlint", "-Ywarn-dead-code", "-Xmax-classfile-name", "130")

// Because we can't run two spark contexts on same VM
parallelExecution in Test := false

libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.0.0" % "provided")
  .exclude("org.apache.hadoop", "hadoop-client")
  .exclude("org.slf4j", "slf4j-log4j12")


libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "2.7.2" % "provided")

libraryDependencies += ("org.apache.hadoop" % "hadoop-aws" % "2.7.2")
  .exclude("org.apache.htrace", "htrace-core")
  .exclude("commons-beanutils", "commons-beanutils")
  .exclude("org.slf4j", "slf4j-log4j12")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"

libraryDependencies += ("org.apache.hadoop" % "hadoop-aws" % "2.7.2")
  .exclude("org.apache.htrace", "htrace-core")
  .exclude("commons-beanutils", "commons-beanutils")
  .exclude("org.slf4j", "slf4j-log4j12")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.9"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.4"

libraryDependencies += "org.joda" % "joda-convert" % "1.7"

libraryDependencies += "commons-lang" % "commons-lang" % "2.6"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += Resolver.sonatypeRepo("public")
