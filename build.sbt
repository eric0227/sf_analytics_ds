import sbt.ExclusionRule

name := "sf_analytics_ds"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.9"

resolvers += "hortonworks Repository" at "http://repo.hortonworks.com/content/groups/public/"

libraryDependencies += "org.spark-project.spark" % "unused" % "1.0.0"  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion  //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion  //% "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0" //% "provided"
libraryDependencies += "org.apache.phoenix" % "phoenix-spark" % "4.14.0-HBase-1.2" excludeAll ExclusionRule(organization = "org.apache.spark") //% "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.9" excludeAll ExclusionRule(organization = "com.fasterxml.jackson")
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

/*
libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0-alpha4" excludeAll(
  ExclusionRule(organization = "junit"),
  ExclusionRule(organization = "com.google.code.findbugs"),
  ExclusionRule(organization = "com.fasterxml.jackson.module"),
  ExclusionRule(organization = "org.apache.hadoop")
)
*/


// in assembly
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case PathList("com", "sun", xs@_*) => MergeStrategy.last
  case PathList("javax", "el", xs@_*) => MergeStrategy.last
  case PathList("javax", "ws", xs@_*) => MergeStrategy.last
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.last
  case PathList("com", "fasterxml", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "application.conf" => MergeStrategy.last
  case "logback.xml"      => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

