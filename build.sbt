name := "apache-spark-2.0-scala"

version := "0.1"

scalaVersion := "2.11.12"

// libraryDependencies += "com.typesafe" % "config" % "1.3.2"

// libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0"
// libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"

// libraryDependencies += "com.maxmind.geoip2" % "geoip2" % "2.12.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
// libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
// libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0"

// libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.8"
// libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.8"
// libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.8"
// libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.1.8"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5"