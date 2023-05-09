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
resolvers ++= Seq(
//  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/maven-releases/",
  "Sonatype OSS Snapshots Repository" at "https://oss.sonatype.org/content/groups/public"
)

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.5")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.5.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.2")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

addSbtPlugin("com.typesafe.play" %% "sbt-plugin" % "2.8.9")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
