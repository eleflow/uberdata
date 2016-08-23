/*
* Copyright 2015 eleflow.com.br.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package eleflow.uberdata.core.conf

import org.apache.spark.SparkConf

/**
  * Created by dirceu on 26/11/15.
  */
object UberdataEventConfig {

  def rootFolder() = {
    "///tmp/Uberdata/"
  }

  def buildPathName(sparkConf: SparkConf) = {
    val appId = sparkConf.getAppId
    val appName = sparkConf.get("spark.app.name")
    s"${buildPath(sparkConf)}/$appName/$appId-${System.currentTimeMillis()}"
  }

  def buildPath(sparkConf:SparkConf) = {
    val host = sparkConf.get("spark.driver.host")
    val customFolder = sparkConf.getOption("uberdata.event.folder").getOrElse("/tmp/")
    s"$customFolder/$host"
  }
}
