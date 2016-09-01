//
// Copyright 2015 eleflow.com.br.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

parallelExecution in Test := false

resolvers += Resolver.mavenLocal

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"

//TODO when moving spark to 1.5, we can update to scala version 2.11.7 according to this issue https://issues.apache.org/jira/browse/SPARK-8013
val scalaV = "2.10.6"

lazy val zeppelin_version = "0.6.0"
lazy val sparkVersion = "1.6.2"
lazy val mysqlV = "5.1.34"

scalaVersion := scalaV

lazy val iuberdata_core = project settings (libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.8.9.1" excludeAll ExclusionRule(
      organization = "com.fasterxml.jackson.core"),
    "joda-time" % "joda-time" % "2.5",
    "org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4" % "provided",
    "com.typesafe" % "config" % "1.3.0",
    "org.scalatest" %% "scalatest" % "2.2.4",
    "org.easymock" % "easymock" % "3.4" % "test",
    "com.typesafe.play" %% "play-json" % "2.4.6",
    "com.cloudera.sparkts" % "sparkts" % "0.3.0" % "provided",
    "ml.dmlc" % "xgboost4j" % "0.5" % "provided",
    "mysql" % "mysql-connector-java" % mysqlV % "runtime"
  )) settings (dependencyOverrides ++= Set(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
  )) enablePlugins JavaAppPackaging

lazy val iuberdata_zeppelin = project dependsOn (iuberdata_core % "test->test;compile->compile") settings
    (libraryDependencies ++= Seq(
      "org.apache.zeppelin" % "zeppelin-interpreter" % zeppelin_version % "provided",
      "org.apache.zeppelin" % "zeppelin-spark" % zeppelin_version % "provided" excludeAll ExclusionRule(
        organization = "org.rosuda.REngine"),
      "org.apache.zeppelin" % "zeppelin-shell" % zeppelin_version excludeAll ExclusionRule(
        organization = "org.mortbay.jetty") excludeAll ExclusionRule(organization = "org.slf4j"),
      "org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "com.google.code.gson" % "gson" % "2.2",
      "commons-collections" % "commons-collections" % "3.2.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4" % "provided",
      "org.scala-lang" % "scala-compiler" % scalaV,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
      "org.apache.maven" % "maven-plugin-api" % "3.0" exclude ("org.codehaus.plexus", "plexus-utils")
        exclude ("org.sonatype.sisu", "sisu-inject-plexus")
        exclude ("org.apache.maven", "maven-model"),
      "org.apache.maven.wagon" % "wagon-http-lightweight" % "1.0" exclude ("org.apache.maven.wagon", "wagon-http-shared"),
      "org.apache.maven.wagon" % "wagon-http" % "1.0",
      "org.rosuda.REngine" % "REngine" % "2.1.1-SNAPSHOT",
      "org.rosuda.REngine" % "Rserve" % "1.8.2-SNAPSHOT",
      "junit" % "junit" % "4.11" % "test"
    )) settings (dependencyOverrides ++= Set(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
  ))

lazy val iuberdata_addon_zeppelin = project settings (libraryDependencies ++= Seq(
    "org.apache.zeppelin" % "zeppelin-zengine" % zeppelin_version % "provided",
    "org.scala-lang" % "scala-reflect" % scalaV
  )) settings (dependencyOverrides ++= Set(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
  )) enablePlugins JavaAppPackaging

test in assembly := {}

testOptions in Test in iuberdata_core += Tests.Argument("-oDF")

parallelExecution in Test in iuberdata_core := false

scalafmtConfig in ThisBuild := Some(file(".scalafmt"))
