organization := "com.wolt.osm"
homepage := Some(url("https://github.com/woltapp/spark-osm-datasource"))
scmInfo := Some(ScmInfo(url("https://github.com/woltapp/spark-osm-datasource"), "git@github.com:woltapp/spark-osm-datasource.git"))
developers := List(Developer("akashihi",
  "Denis Chaplygin",
  "akashihi@gmail.com",
  url("https://github.com/akashihi")))
licenses += ("Apache2", url("https://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

name := "spark-osm-tools"
version := "0.1.0"

scalaVersion := "2.11.12"
crossScalaVersions := Seq("2.12.10")

val mavenLocal = "Local Maven Repository" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
resolvers += mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "com.wolt.osm" %% "spark-osm-datasource" % "0.3.0",
  "org.locationtech.jts" % "jts-core" % "1.16.1",
  "org.scalatest" %% "scalatest" % "3.0.8" % "it,test",
  "org.scalactic" %% "scalactic" % "3.0.8" % "it,test",
  "com.vividsolutions" % "jts" % "1.13",
  "org.postgresql" % "postgresql" % "42.2.8"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
