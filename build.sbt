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

val scalaV = "2.13.10"

//WARNING: changing the zeppelin version requires changing the dependency version in setup_zeppelin_local.sh and iuberdata.sh
lazy val zeppelin_version = "0.7.1"
lazy val sparkVersion = "3.3.2"
val sparkV: sbt.SettingKey[scala.Predef.String] = SettingKey(sparkVersion)
lazy val mysqlV = "5.1.34"
lazy val slf4jVersion = "2.0.6"
lazy val circeVersion = "0.14.5"
lazy val hiveVersion = "2.3.9"
lazy val xgboostVersion = "0.8-SNAPSHOT"

lazy val commonSettings = Seq(
	organization := "br.com.eleflow",
	version := "0.3.0-SNAPSHOT",
	scalaVersion := scalaV
)

isSnapshot := true

lazy val publishRepo = {
	val nexus = "http://10.44.1.88:8080/archiva/repository"
	//  if (snap)
	Some("snapshots" at nexus + "/snapshots")
	//  else
	//    Some("releases"  at nexus + "/internal")
}

publishTo in ThisBuild := publishRepo

resolvers ++= Seq(
//	"Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
	"Objective Nexus Snapshots" at "http://10.44.1.88:8080/archiva/repository/snapshots",
	"Objective Nexus Release" at "http://10.44.1.88:8080/archiva/repository/internal")
scalaVersion := scalaV

lazy val iuberdata_core = project settings(
	isSnapshot := true,
	publishTo := publishRepo,
	libraryDependencies ++= Seq(
		"com.amazonaws" % "aws-java-sdk" % "1.8.9.1" excludeAll ExclusionRule(
			organization = "com.fasterxml.jackson.core"),
		"joda-time" % "joda-time" % "2.5",
		"org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion % "provided",
		"org.apache.hive.hcatalog" % "hive-hcatalog-core" % hiveVersion % "provided" excludeAll ExclusionRule
		(organization = "org.pentaho"),
		"org.apache.hive.hcatalog" % "hive-hcatalog-streaming" % hiveVersion % "provided",
		"com.typesafe" % "config" % "1.3.0",
		"org.scalatest" %% "scalatest" % "3.2.15",
		"org.easymock" % "easymock" % "3.4" % "test",
		"com.typesafe.play" %% "play-json" % "2.9.4" excludeAll ExclusionRule(
			organization = "com.fasterxml.jackson.core") excludeAll ExclusionRule(
			organization = "com.fasterxml.jackson.datatype"),
		"com.cloudera.sparkts" % "sparkts" % "0.4.0-GABRIEL" % "provided",
//		"ml.dmlc" % "xgboost4j" % xgboostVersion % "provided" excludeAll ExclusionRule(
//			organization = "com.esotericsoftware.minlog"),
//		"ml.dmlc" % "xgboost4j-spark" % xgboostVersion % "provided" excludeAll ExclusionRule(
//			organization = "com.esotericsoftware.minlog"),
		"ml.dmlc" % "xgboost4j_2.12" % "1.7.4" excludeAll ExclusionRule(
			organization = "com.esotericsoftware.minlog"),
		"ml.dmlc" % "xgboost4j-spark_2.12" % "1.7.4" excludeAll ExclusionRule(
			organization = "com.esotericsoftware.minlog"),
		//    "com.databricks" %% "spark-csv" % "1.5.0",
		"org.slf4j" % "slf4j-api" % slf4jVersion,
		"org.slf4j" % "slf4j-log4j12" % slf4jVersion,
		"io.circe" %% "circe-core" % circeVersion,
		"io.circe" %% "circe-generic" % circeVersion,
		"io.circe" %% "circe-parser" % circeVersion,
		"org.apache.hive" % "hive-jdbc" % hiveVersion % "provided",
		"ai.x" %% "play-json-extensions" % "0.42.0" excludeAll ExclusionRule(
			organization = "com.fasterxml.jackson.core") excludeAll ExclusionRule(
			organization = "com.fasterxml.jackson.module"),
		"org.apache.thrift" % "libthrift" % "0.18.1"
	)) settings(
		commonSettings,
		dependencyOverrides ++= Set(
			"com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
			"com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
			"com.fasterxml.jackson.module" % "jackson-module-scala" % "2.8.7")
	)enablePlugins JavaAppPackaging

lazy val iuberdata_zeppelin = project dependsOn (iuberdata_core % "test->test;compile->compile") settings
	(commonSettings, libraryDependencies ++= Seq(
		"org.apache.zeppelin" % "zeppelin-interpreter" % zeppelin_version excludeAll ExclusionRule(
			organization = "asm"),
		"org.jsoup" % "jsoup" % "1.8.2",
		"org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
		"com.google.code.gson" % "gson" % "2.2",
		"commons-collections" % "commons-collections" % "3.2.1",
		"org.scala-lang" % "scala-compiler" % scalaV,
		"org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
		"org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
		"org.apache.maven" % "maven-plugin-api" % "3.0" exclude("org.codehaus.plexus", "plexus-utils")
			exclude("org.sonatype.sisu", "sisu-inject-plexus")
			exclude("org.apache.maven", "maven-model"),
		"org.apache.maven.wagon" % "wagon-http-lightweight" % "1.0" exclude("org.apache.maven.wagon", "wagon-http-shared"),
		"org.apache.maven.wagon" % "wagon-http" % "1.0",
		"org.rosuda.REngine" % "REngine" % "2.1.1-SNAPSHOT",
		"org.rosuda.REngine" % "Rserve" % "1.8.2-SNAPSHOT"
		, "org.apache.zeppelin" % "zeppelin-interpreter" % zeppelin_version % "provided"
		, "junit" % "junit" % "4.11" % "test"
	)) settings (commonSettings)
//, dependencyOverrides ++= Set(
//    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
//  ))

lazy val iuberdata_addon_zeppelin = project settings (libraryDependencies ++= Seq(
	"org.apache.zeppelin" % "zeppelin-zengine" % zeppelin_version % "provided",
	"org.scala-lang" % "scala-reflect" % scalaV
)) settings (commonSettings) enablePlugins JavaAppPackaging
//, dependencyOverrides ++= Set(
//    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
//  ))

//test in assembly := {}

testOptions in Test in iuberdata_core += Tests.Argument("-oDF")

parallelExecution in Test in iuberdata_core := false

//scalafmtConfig in ThisBuild := Some(file(".scalafmt"))
