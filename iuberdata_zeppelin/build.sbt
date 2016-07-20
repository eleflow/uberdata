import sbt.Keys._

val nm = "eleflow.uberdata.IUberdata-Zeppelin"

val ver = "0.1.0"

name := nm

version := ver

test in assembly := {}

assemblyJarName in assembly := s"$nm-$ver.jar"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers in ThisBuild += Resolver.mavenLocal

resolvers in ThisBuild  += Resolver.sonatypeRepo("snapshots")
