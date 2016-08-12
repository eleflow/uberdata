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
package org.apache.spark.rpc.netty

import eleflow.uberdata.core.IUberdataContext
import eleflow.uberdata.core.util.ClusterSettings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkEnv}
import org.scalatest.{BeforeAndAfterEach, Suite}

object TestSparkConf {
   def conf = {
    val sconf = new SparkConf()
    sconf.set("spark.app.name", "teste")
    sconf
  }

  val separator =","

}

/**
 * Created by dirceu on 22/10/14.
 */
trait BeforeAndAfterWithContext extends BeforeAndAfterEach {
  this: Suite =>

  val defaultFilePath = "src/test/resources/"
  import TestSparkConf._
  ClusterSettings.master=Some("local[*]")
  conf.set("spark.driver.allowMultipleContexts","true")
  val context = IUberdataContext.getUC(conf)

  override def beforeEach() = {
    setLogLevels(Level.INFO, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map {
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

  override def afterEach() = {
    val get = SparkEnv.get
    val rpcEnv =
    if(get!=null) {
      Some(get.rpcEnv)
    }else None
    context.clearContext()
    rpcEnv.foreach(_.fileServer.asInstanceOf[org.apache.spark.rpc.netty.HttpBasedFileServer].shutdown())

    System.clearProperty("spark.master.port")
  }
}

