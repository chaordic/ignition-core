name := "Ignition-Core"

version := "1.0"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings", "-Xlint", "-Ywarn-dead-code", "-Xmax-classfile-name", "130")

// Because we can't run two spark contexts on same VM
parallelExecution in Test := false

//test in assembly := {}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.6" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.6" % "provided"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4" % "provided"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.27"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

libraryDependencies += "org.joda" % "joda-convert" % "1.8.2"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3"
