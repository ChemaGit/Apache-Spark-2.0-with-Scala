name := "apache-spark-2.0-scala"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0"

//(unmanagedResources in Compile) := (unmanagedResources in Compile).value.filter(name => List("exercises_cert_1","exercises_cert").contains(name) )
/*excludeFilter in unmanagedResources := {
  val public = ((resourceDirectory in Compile).value / "exercises_cert").getCanonicalPath
  new SimpleFileFilter(_.getCanonicalPath startsWith public)
}*/

// excludeFilter in unmanagedSources := HiddenFileFilter || "*exercise*"