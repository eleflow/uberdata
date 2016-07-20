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
