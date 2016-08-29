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

package eleflow.uberdata.core.reader

import java.net.URI
import java.nio.ByteBuffer

import eleflow.uberdata.core.IUberdataContext
import eleflow.uberdata.core.conf.UberdataEventConfig
import eleflow.uberdata.core.data.json._
import eleflow.uberdata.core.enums.SparkJsonEnum
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.sql.SQLContext

import org.json4s.DefaultFormats

/**
  * Created by dirceu on 26/11/15.
  */
object UberdataEventReader {

  lazy val conf = new Configuration()
  lazy val fs = FileSystem.get(new URI(UberdataEventConfig.rootFolder()), conf)
  implicit val formats = DefaultFormats

  def parseJson(jsonName: String, json: String) = {
    try {
      import eleflow.uberdata.core.json.SparkJsonMapper._
      import play.api.libs.json._
      jsonName match {
        case value if value == SparkJsonEnum.JobEnd.toString =>
          Json.fromJson[JobEnd](Json.parse(json)).get // Serialization.read[JobEnd](json)
        case value if value == SparkJsonEnum.JobStart.toString =>
          Json.fromJson[JobStart](Json.parse(json)).get // Serialization.read[JobStart](json)
        case value
            if value == SparkJsonEnum.StageCompleted.toString || value == SparkJsonEnum.StageSubmitted.toString =>
          Json.fromJson[Stage](Json.parse(json)).get // Serialization.read[Stage](json)
        case value if value == SparkJsonEnum.TaskEnd.toString =>
          Json
            .fromJson[Map[String, String]](Json.parse(json))
            .get // Serialization.read[TaskEnd](json)
        case value if value == SparkJsonEnum.TaskStart.toString =>
          Json.fromJson[TaskStart](Json.parse(json)).get // Serialization.read[TaskStart](json)
        case value if value == SparkJsonEnum.TaskGettingResult.toString =>
          Json
            .fromJson[TaskGettingResult](Json.parse(json))
            .get // Serialization.read[TaskGettingResult](json)
        case value if value == SparkJsonEnum.AccumulableInfo.toString =>
          Json
            .fromJson[AccumulableInfo](Json.parse(json))
            .get // Serialization.read[AccumulableInfo](json)
        case _ =>
          new NoSuchElementException(
            s"Errow when trying to read Uberdata usage json, name not found $jsonName"
          )
      }
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def readEvents(uc: IUberdataContext, sqlContext: SQLContext): Unit = {
    val path = new Path(UberdataEventConfig.buildPath(uc.sparkContext.getConf))

    lazy val fs = FileSystem.get(path.toUri, conf)

    val hosts: Seq[FileStatus] = fs.listStatus(path).toSeq

    val apps = hosts.map { status =>
      val path = status.getPath
      path.getName -> fs.listStatus(path)
    }

    val values = apps.flatMap {
      case (app, files) =>
        files.flatMap { fileStatus =>
          val fileName =
            fileStatus.getPath.toUri.toString.replace("file:///", "///")
          readFile(fileName, Some(fs)).map { line =>
            val Array(eventName, json) = line.split("\t")
            (eventName, json)
          }

        }
    }
    val grouped = uc.sparkContext.parallelize(values).cache
    grouped.keys.distinct.collect.foreach { tableName =>
      sqlContext.read.json(grouped.filter(_._1 == tableName).values).registerTempTable(tableName)
    }
  }

  def readFile(pathName: String, fs: Option[FileSystem]): Seq[String] = {
    readFile(new Path(pathName), fs)
  }

  def readFile(path: Path, fs: Option[FileSystem]): Seq[String] = {
    val fileSystem = fs.getOrElse(this.fs)
    val inputStream = fileSystem.open(path)
    val stringBuilder =
      scala.io.Source.fromInputStream(inputStream).getLines.map { value =>
        value.replace("\n", "")
      }
    stringBuilder.toSeq
  }

  def readString(buffer: ByteBuffer, stringBuilder: StringBuilder) = {
    val array = new Array[Byte](buffer.limit())
    buffer.get(array)
    stringBuilder.append(new String(array))
  }
}
