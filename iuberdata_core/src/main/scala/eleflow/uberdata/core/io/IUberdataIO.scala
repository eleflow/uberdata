/*
 *  Copyright 2015 eleflow.com.br.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package eleflow.uberdata.core.io

import java.io.{File, InputStream, OutputStream}
import java.net.URI

import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import com.amazonaws.services.s3.model.{
  GetObjectRequest,
  ObjectMetadata,
  PutObjectRequest,
  S3Object
}
import eleflow.uberdata.core.IUberdataContext
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.util.Try

/**
  * Created by dirceu on 22/09/16.
  */
object IUberdataIO {

  def apply() = new IUberdataIO()
}

class IUberdataIO extends Serializable {
  @transient lazy val sparkContext = IUberdataContext.getUC.sparkContext
  @transient protected lazy val s3Client: AmazonS3 = new AmazonS3Client()
  protected def createPathInstance(input: String) = new Path(input)

  def copyDir(inputFiles: Seq[String], output: String): Unit = {
    sparkContext.parallelize(inputFiles).foreach { inputFile =>
      val from = new URI(inputFile)

      copy(inputFile, s"$output/${from.getPath}")
    }
  }

  def copy(input: String, output: String): Unit = {
    val from = new URI(input)
    val to = new URI(output)
    val fromScheme = from.getScheme
    val toScheme = to.getScheme
    val conf = new Configuration()

    (fromScheme, toScheme) match {
      case ("s3n" | "s3", "s3n" | "s3") => ???
      case (fromAddr, _) if fromAddr.startsWith("s3") =>
        val outputPath = createPathInstance(output)
        val fs = outputPath.getFileSystem(conf)
        copyFromS3(from, outputPath, fs)
      case _ =>
        val srcPath = createPathInstance(input)
        val srcFs = srcPath.getFileSystem(conf)
        val dstPath = createPathInstance(output)
        val dstFs = dstPath.getFileSystem(conf)
        FileUtil.copy(srcFs, srcPath, dstFs, dstPath, false, conf)
    }
  }

  def copy(input: File, output: String): Boolean = {
    val conf = new Configuration()
    val dstPath = createPathInstance(output)
    val dstFs = dstPath.getFileSystem(conf)
    FileUtil.copy(input, dstFs, dstPath, false, conf)
  }

  protected def copyFromS3(input: URI, path: Path, fs: FileSystem): Unit = {
    val inputStream = readFromS3(input)
    inputStream.map { in =>
      val copyResult = Try(fs.create(path)).flatMap { out =>
        val copyResult = copyStreams(in, out)
        out.close()
        copyResult
      }.recover {
        case e: Exception =>
          e.printStackTrace()
          println(e.getStackTrace.mkString("\n"))
          throw e
      }
      in.close()
      copyResult
    }.recover {
      case e: Exception =>
        e.printStackTrace()
        println(e.getStackTrace.mkString("\n"))
        throw e
    }
  }

  protected def copyToS3(input: Path, bucket: String, fileName: String): Unit = {

    val objRequest = new PutObjectRequest(
      bucket,
      fileName,
      readFromHDFS(input),
      new ObjectMetadata()
    )
    s3Client.putObject(objRequest)
  }

  def readFromS3(input: URI): Try[InputStream] = {
    val rangeObjectRequest: GetObjectRequest =
      new GetObjectRequest(input.getHost, input.getPath.substring(1))
    Try {

      val objectPortion: S3Object = s3Client.getObject(rangeObjectRequest)
      objectPortion.getObjectContent
    }
  }

  private def readFromHDFS(input: Path) = {
    val fs = input.getFileSystem(new Configuration)
    fs.open(input)
  }

  protected def copyStreams(in: InputStream, out: OutputStream) =
    Try(IOUtils.copy(in, out))

  def fs(pathStr: String): FileSystem = {
    val path = createPathInstance(pathStr)
    path.getFileSystem(new Configuration)
  }

}
