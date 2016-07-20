import sbt._
import sbt.Keys._
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.SbtGit._

versionWithGit

organization := "eleflow"

version :="0.1.0"

resolvers in ThisBuild += Resolver.mavenLocal

testOptions in Test += Tests.Argument("-oDF")

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Snapshots Repository" at "http://oss.sonatype.org/content/groups/public"
)