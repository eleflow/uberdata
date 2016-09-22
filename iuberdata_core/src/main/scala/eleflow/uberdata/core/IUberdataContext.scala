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
package eleflow.uberdata.core

import scala.collection.JavaConverters._
import java.io._

import eleflow.uberdata.core.listener.UberdataSparkListener
import eleflow.uberdata.core.data.Dataset
import eleflow.uberdata.core.util.ClusterSettings
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.{Logging, SparkConf, SparkContext}
import ClusterSettings._
import org.apache.spark.sql.DataFrame

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.matching.Regex

object IUberdataContext {

  var conf: SparkConf = new SparkConf

  private lazy val uc: IUberdataContext = new IUberdataContext(conf)

  def getUC = uc

  def getUC(conf: SparkConf) = {
    this.conf = conf
    uc
  }

}

/**
  * User: paulomagalhaes
  * Date: 8/15/14 12:24 PM
  */
class IUberdataContext(@transient sparkConf: SparkConf) extends Serializable with Logging {
	protected def this(sparkConf: SparkConf, data: String) = this(sparkConf)
//  @transient protected lazy val s3Client: AmazonS3 = new AmazonS3Client()
  val version = UberdataCoreVersion.version
  protected val basePath: String = "/"
  @transient var _sqlContext: Option[HiveContext] = None
  @transient protected var sc: Option[SparkContext] = None
  private var _masterHost: Option[String] = None

  def initialized: Boolean = sc.isDefined

  def isContextDefined: Boolean = sc.isDefined

  def terminate(): Unit = {
    clearContext()
    val path = getSparkEc2Py
    shellRun(Seq(path, "destroy", clusterName))
    _masterHost = None
    ClusterSettings.resume = false
  }

  def clearContext(): Unit = {
    ClusterSettings.resume = true
    sc.foreach { f =>
      f.cancelAllJobs()
//        HiveThriftServer2.listener.server.stop() // stop ThriftServer
      f.stop()
    }
    _sqlContext = None
    sc = None
  }

  def clusterInfo(): Unit = {
    val path = getSparkEc2Py
    shellRun(Seq(path, "get-master", clusterName))
  }

  def shellRun(command: Seq[String]): String = {
    val out = new StringBuilder

    val logger = ProcessLogger((o: String) => {
      out.append(o)
      logInfo(o)
    }, (e: String) => {
      println(e)
      logInfo(e)
    })
    command ! logger
    out.toString()
  }

  def reconnect(): Unit = {
    sc.foreach(_.stop())
    sc = None
    _sqlContext = None
  }

  def getAllFilesRecursively(fullPath: Path): Seq[String] = {
    val fs = fullPath.getFileSystem(new Configuration)
    @tailrec
    def iter(fs: FileSystem, paths: Seq[Path], result: Seq[String]): Seq[String] = paths match {
      case path :: tail =>
        val children: Seq[FileStatus] = try {
          fs.listStatus(path)
        } catch {
          case e: FileNotFoundException =>
            // listStatus throws FNFE if the dir is empty
            Seq.empty[FileStatus]
        }
        val (files, directories) = children.partition(_.isFile)
        iter(
          fs,
          tail ++ directories.map(_.getPath),
          files.map(_.getPath.toString) ++ result
        )
      case _ =>
        result
    }
    iter(fs, Seq(fullPath), Seq())
  }

  def sparkContext: SparkContext = sc getOrElse {
    val context =
      if (ClusterSettings.master.isDefined) {
				createSparkContextForProvisionedCluster(sparkConf)
			} else {
				createSparkContextForNewCluster(sparkConf)
			}
    addClasspathToSparkContext(context)
    sc = Some(context)
    val listener = new UberdataSparkListener(context.getConf)
    context.addSparkListener(listener)
    context
  }

  def addClasspathToSparkContext(context: SparkContext): Unit = {
    val sqoop = "org.apache.sqoop.sqoop-.*jar".r
    val jodaJar = "joda-time.joda-time-.*jar".r
    val eleflowJar = "eleflow.*jar".r
    val guavaJar = "com.google.guava.*".r
    val mySqlDriver = "mysql-connector-java.*".r
    val oracle = "ojdbc6.*".r
    val urls = this.getClass.getClassLoader.asInstanceOf[java.net.URLClassLoader].getURLs
    val jarUrls = urls.filter(
      url =>
        sqoop.findFirstIn(url.getFile).isDefined
          || jodaJar.findFirstIn(url.getFile).isDefined
          || eleflowJar.findFirstIn(url.getFile).isDefined
          || guavaJar.findFirstIn(url.getFile).isDefined
          || mySqlDriver.findFirstIn(url.getFile).isDefined
          || oracle.findFirstIn(url.getFile).isDefined
    )
    jarUrls.foreach { url =>
      logInfo(s"adding ${url.getPath} to spark context jars")
      context.addJar(url.getPath)
    }
  }

  def createSparkContextForNewCluster(conf: SparkConf): SparkContext = {
    log.info(s"connecting to $masterHost")
    conf.setMaster(s"spark://$masterHost:7077")
    confSetup(conf)
  }

  def masterHost: String = {
    _masterHost match {
      case Some(host) => host
      case None =>
        initHostNames()
        _masterHost.get

    }
  }

  def initHostNames(): Unit = {
    _masterHost = createCluster
  }

  def createCluster: Option[String] = {
    val path = getSparkEc2Py
    val mandatory = Seq(
      path,
      "--hadoop-major-version",
      hadoopVersion,
      "--master-instance-type",
      masterInstanceType,
      "--slaves",
      coreInstanceCount.toString,
      "--instance-type",
      coreInstanceType
    )
    val command = mandatory ++ (ec2KeyName match {
      case None => Seq[String]()
      case Some(keyName) => Seq("--key-pair", keyName)
    }) ++ (spotPriceFactor match {
      case None => Seq[String]()
      case Some(spotPrice) => Seq("--spot-price", spotPrice)
    }) ++ (region match {
      case None => Seq[String]()
      case Some(awsRegion) => Seq("--region", awsRegion)
    }) ++ (profile match {
      case None => Seq[String]()
      case Some(awsProfile) => Seq("--profile", awsProfile)
    }) ++ (if (resume) Seq("--resume") else Seq())

    val output = shellRun(command ++ Seq("launch", clusterName))

    log.info(s"Output:: $output")
    val pattern = new Regex(
      "Spark standalone cluster started at http://([^:]+):8080"
    )
    val host = pattern.findAllIn(output).matchData.map(_.group(1)).next
    Some(host)
  }

  def masterHost_=(host: String): Unit = _masterHost = Some(host)

  private def confSetup(conf: SparkConf): SparkContext = {
    ClusterSettings.additionalConfs.map {
      case (key, value) => conf.set(key, value)
    }
    conf.set("spark.app.name", ClusterSettings.appName)
    conf.set("spark.sql.parquet.compression.codec", "snappy")
    conf.set("spark.local.dir", ClusterSettings.localDir)
    conf.set("spark.externalBlockStore.baseDir", ClusterSettings.baseDir)
    conf.set("spark.task.cpus", ClusterSettings.taskCpus.toString)
    ClusterSettings.defaultParallelism.map(
      value => conf.set("spark.default.parallelism", value.toString)
    )
    ClusterSettings.kryoBufferMaxSize.map(
      value => conf.set("spark.kryoserializer.buffer.max", value.toString)
    )
    //according to keo, in Making Sense of Spark Performance webcast, this codec is better than default
    conf.set("spark.io.compression.codec", "lzf")
    conf.set("spark.driver.maxResultSize", ClusterSettings.maxResultSize)
    conf.set(
      "spark.serializer",
      ClusterSettings.serializer.getOrElse(
        "org.apache.spark.serializer.KryoSerializer"
      )
    )

    val defaultConfStream =
      this.getClass.getClassLoader.getResourceAsStream("spark-defaults.conf")
    if (defaultConfStream != null) {

      val defaultConf = IOUtils.readLines(defaultConfStream)
      defaultConf.asScala.map { line =>
        val keyValue = line.split("\\s+")
        if (keyValue.size == 2)
          conf.set(keyValue(0), keyValue(1))
      }
    }
    //according to keo, in Making Sense of Spark Performance webcast, this codec is better than default
    conf.set("spark.io.compression.codec", "lzf")

    conf.set("spark.driver.maxResultSize", ClusterSettings.maxResultSize)
    ClusterSettings.executorMemory.foreach(
      conf.set("spark.executor.memory", _)
    )
    new SparkContext(conf)
  }

  def createSparkContextForProvisionedCluster(conf: SparkConf): SparkContext = {
    log.info("connecting to localhost")
    conf.setMaster(ClusterSettings.master.get)
    confSetup(conf)
  }

  def fs(pathStr: String): FileSystem = {
    val path = createPathInstance(pathStr)
    path.getFileSystem(new Configuration)
  }

  protected def createPathInstance(input: String) = new Path(input)

  def sql(sql: String): DataFrame = {
    sqlContext.sql(sql)
  }

  def sqlContext: HiveContext = {
    _sqlContext match {
      case None =>
        _sqlContext = Some(new HiveContext(sparkContext))
        HiveThriftServer2.startWithContext(_sqlContext.get)
        _sqlContext.get

      case Some(ctx) => ctx
    }
  }

  def load(file: String, separator: String = ","): Dataset =
    Dataset(this, file, separator)

  private def copyFromClasspath2Tmp(filePath: String) = {
    val scriptPath = System.getProperty("java.io.tmpdir")
    val classLoader: ClassLoader = getClass.getClassLoader
    val out: File = new File(s"$scriptPath/$filePath")
    if (out.exists && out.isDirectory) {
      throw new RuntimeException(
        "Can't create python script " + out.getAbsolutePath
      )
    }
    if (!out.getParentFile.exists()) {
      out.getParentFile.mkdirs()
    }
    try {
      val outStream: FileOutputStream = new FileOutputStream(out)
      IOUtils.copy(classLoader.getResourceAsStream(filePath), outStream)
      outStream.close()
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
    out
  }

  private def getSparkEc2Py = {
    copyFromClasspath2Tmp(
      "python/deploy.generic/root/spark-ec2/ec2-variables.sh"
    ).toString
    val path = copyFromClasspath2Tmp("python/spark_ec2.py")
    path.setExecutable(true)
    log.info(s"spark_ec2.py in $path")
    path.toString
  }
}
