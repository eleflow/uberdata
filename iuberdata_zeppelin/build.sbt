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

import sbt.Keys._

val nm = "eleflow.uberdata.IUberdata-Zeppelin"

val ver = "0.2.0"

name := nm

version := ver

test in assembly := {}

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers in ThisBuild += Resolver.mavenLocal

resolvers in ThisBuild += Resolver.sonatypeRepo("snapshots")

sourceGenerators in Compile <+= (sourceManaged in Compile, version, name) map { (d, v, n) =>
  val file = d / "UberdataJarVersion.scala"
  IO.write(file, """package eleflow.uberdata.core
    |object UberdataJarVersion {
    |  val version = "%s"
    |  val name = "%s"
		|  val jarName = s"${name}-${version}.jar"
    |}
    |""".stripMargin.format(v, n))
  Seq(file)
}

