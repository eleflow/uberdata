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