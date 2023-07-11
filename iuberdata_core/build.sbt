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

import com.typesafe.sbt.SbtGit._
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.SbtNativePackager.autoImport.NativePackagerKeys._
import sbt.Keys._
import sbt._

versionWithGit

val ver = "0.2.0"

//lazy val publishRepo = Some("Objective Nexus Snapshots" at
//  "http://repo:8080/archiva/repository/snapshots")
//
//publishTo := publishRepo
publishMavenStyle := true

resolvers += Resolver.sonatypeRepo("public")

resolvers += Resolver.typesafeIvyRepo("releases")

resolvers += Resolver.sonatypeRepo("releases")

resolvers += Resolver.mavenLocal

//resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases"
resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/ivy-releases/"
testOptions in Test += Tests.Argument("-oDF")
parallelExecution in Test := false
net.virtualvoid.sbt.graph.Plugin.graphSettings

def iUberdataCoreVersion(gitVersion: Option[String] = Some("Not a Git Repository"), dir: File) = {
	val file = dir / "UberdataCoreVersion.scala"
	IO.write(
		file,
		s"""package eleflow.uberdata.core\n  object UberdataCoreVersion{\n          val gitVersion =
			 |"${gitVersion.get}"\n
			 |}\n""".stripMargin)
	Seq(file)

}


sourceGenerators in Compile += ((git.gitHeadCommit, sourceManaged in Compile) map
	iUberdataCoreVersion).taskValue