ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

val flinkVersion = "1.18.0"
val jacksonVersion = "2.15.1"
val scalatestVersion = "3.2.15"
val mockitoVersion = "5.5.0"
val powerMockitoVersion = "2.0.9"
val junitTestVersion = "4.13.2"
val testcontainerVersion = "1.19.3"

lazy val root = (project in file("."))
        .settings(
            name := "flink-demo",
            idePackagePrefix := Some("com.hismalltree.demo.flink")
        )

libraryDependencies ++= Seq(

    "org.apache.flink" % "flink-clients" % flinkVersion,
    "org.apache.flink" % "flink-java" % flinkVersion,
    "org.apache.flink" % "flink-streaming-java" % flinkVersion,

    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,

    "junit" % "junit" % junitTestVersion % Test,
    "org.apache.flink" % "flink-test-utils" % flinkVersion % Test,
    "org.apache.flink" % "flink-table-test-utils" % flinkVersion % Test,
    "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    "org.mockito" % "mockito-core" % mockitoVersion % Test,
    "org.powermock" % "powermock-api-mockito2" % powerMockitoVersion % Test,
    "org.powermock" % "powermock-module-junit4" % powerMockitoVersion % Test,

    "org.testcontainers" % "testcontainers" % testcontainerVersion % Test,
    "org.testcontainers" % "mysql" % testcontainerVersion % Test,



)
