import sbt._
import sbt.Keys._

object ProjectBuild extends Build {

  lazy val project = Project(
    id = "jsondb",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "JsonDB",
      organization := "net.mardambey",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.0",
			javaOptions in run +=	"-Xmx1G", 
			fork in run := true,
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
			resolvers += "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.1.1",
			libraryDependencies += "ognl" % "ognl" % "3.0.4",
			libraryDependencies += "javax.servlet" % "servlet-api" % "2.5",
			libraryDependencies += "com.jolbox" % "bonecp" % "0.8.0-rc1",
			libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24",
			libraryDependencies += "org.mapdb" % "mapdb" % "0.9-SNAPSHOT",
			libraryDependencies += "io.netty" % "netty" % "3.6.3.Final",
			libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.1.3"
    )
  )
}
