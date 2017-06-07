name := "Data analysis algorithms"
description := "Library providing algorithms to use in Zeppelin"
version := "0.1"
scalaVersion := "2.11.0"

resolvers ++= Seq(
  "maven" at "https://repo1.maven.org/maven2/",
  "confluent" at "http://packages.confluent.io/maven/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0" % "provided",
  "io.confluent" % "kafka-avro-serializer" % "3.0.0"
    exclude("com.fasterxml.jackson.core", "jackson-core")
    exclude("com.fasterxml.jackson.core", "jackson-databind")
    exclude("com.fasterxml.jackson.core", "jackson-annotations")
    exclude("", "")
    exclude("org.apache.avro", "avro"),
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.5",
  "org.apache.avro" % "avro" % "1.8.0" % "provided",
  "com.esotericsoftware.kryo" % "kryo" % "2.21" % "provided"
)

assemblyJarName in assembly := "algorithms.jar"
