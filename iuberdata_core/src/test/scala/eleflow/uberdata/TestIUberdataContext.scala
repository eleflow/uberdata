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

package eleflow.uberdata

/**
  * Created by dirceu on 30/09/14.
  */
import java.io.IOException
import java.net.URI

import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{S3Object, S3ObjectInputStream}
import eleflow.uberdata.core.io.IUberdataIO
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkConf
import org.easymock.EasyMock
import org.easymock.EasyMock.anyObject
import org.scalatest._

/**
  * Created by dirceu on 30/09/14.
  */
class TestIUberdataContext extends FlatSpec with Matchers {

  val conf = EasyMock.createMock(classOf[SparkConf])
  val host = "10.0.0.1"
  val bucket = "dacpoc"
  val file = "arquivo.csv"
  val hdfspath = s"hdfs://$host/$bucket/$file"
  val s3path = s"s3n:///$bucket/$file"

  "UberdataContext" should "copy files from s3 to hdfs" in {

    val path = EasyMock.createMock(classOf[Path])
    val s3Object = EasyMock.createMock(classOf[S3Object])
    val s3ClientMock = EasyMock.createMock(classOf[AmazonS3])
    val inputStream = EasyMock.createMock(classOf[S3ObjectInputStream])
    val outputStream = EasyMock.createMock(classOf[FSDataOutputStream])
    val fileSystem = EasyMock.createMock(classOf[FileSystem])

    EasyMock.expect(s3ClientMock.getObject(anyObject())).andReturn(s3Object).anyTimes()
    EasyMock.expect(inputStream.read(anyObject())).andReturn(-1).anyTimes()
    EasyMock.expect(s3Object.getObjectContent()).andReturn(inputStream).times(1)
    EasyMock.expect(path.getFileSystem(anyObject())).andReturn(fileSystem).anyTimes()
    EasyMock.expect(fileSystem.create(path)).andReturn(outputStream).times(1)
    EasyMock.expect(outputStream.close()).times(1)
    EasyMock.expect(inputStream.close()).times(1)

    EasyMock.replay(path, s3Object, s3ClientMock, inputStream, outputStream, fileSystem)

    val uberdata = new IUberdataIO {
      override def copy(input: String, output: String): Unit = {
        copyFromS3(new URI(input), path, fileSystem)
      }
      override lazy val s3Client = s3ClientMock
    }
    uberdata.copy(hdfspath, "outputFile")
    EasyMock.verify(path, s3Object, s3ClientMock, inputStream, outputStream, fileSystem)
  }

  it should "Close streams when stream copy throws an exception" in {
    val path = EasyMock.createMock(classOf[Path])
    val s3Object = EasyMock.createMock(classOf[S3Object])
    val s3ClientMock = EasyMock.createMock(classOf[AmazonS3])
    val inputStream = EasyMock.createMock(classOf[S3ObjectInputStream])
    val outputStream = EasyMock.createMock(classOf[FSDataOutputStream])
    val fileSystem = EasyMock.createMock(classOf[FileSystem])

    EasyMock.expect(s3ClientMock.getObject(anyObject())).andReturn(s3Object).anyTimes()
    EasyMock.expect(inputStream.read(anyObject())).andThrow(new IOException).times(1)
    EasyMock.expect(s3Object.getObjectContent()).andReturn(inputStream).times(1)
    EasyMock.expect(path.getFileSystem(anyObject())).andReturn(fileSystem).anyTimes()
    EasyMock.expect(fileSystem.create(path)).andReturn(outputStream).anyTimes()
    EasyMock.expect(inputStream.close()).times(1)
    EasyMock.expect(outputStream.close()).times(1)

    EasyMock.replay(path, s3Object, s3ClientMock, inputStream, outputStream, fileSystem)

    val uberdata = new IUberdataIO {
      override def copy(input: String, output: String): Unit = {
        copyFromS3(new URI(input), path, fileSystem)
      }

      override lazy val s3Client = s3ClientMock
    }

    uberdata.copy(hdfspath, "outputFile")

    EasyMock.verify(path, s3Object, s3ClientMock, inputStream, outputStream, fileSystem)
  }

  it should "handle get S3 object exceptions" in {
    val path = EasyMock.createMock(classOf[Path])
    val s3ClientMock = EasyMock.createMock(classOf[AmazonS3])

    EasyMock
      .expect(s3ClientMock.getObject(anyObject()))
      .andThrow(new AmazonClientException("Testing error"))
      .times(1)

    EasyMock.replay(path, s3ClientMock)

    val uberdata = new IUberdataIO {
      override def copy(input: String, output: String): Unit = {
        copyFromS3(new URI(input), path, EasyMock.createMock(classOf[FileSystem]))
      }

      override lazy val s3Client = s3ClientMock
    }

    uberdata.copy(hdfspath, "outputFile")

    EasyMock.verify(path, s3ClientMock)
  }

  it should "handle getObjectContent exceptions" in {
    val path = EasyMock.createMock(classOf[Path])
    val s3Object = EasyMock.createMock(classOf[S3Object])
    val s3ClientMock = EasyMock.createMock(classOf[AmazonS3])

    EasyMock.expect(s3ClientMock.getObject(anyObject())).andReturn(s3Object).times(1)
    EasyMock
      .expect(s3Object.getObjectContent())
      .andThrow(new AmazonClientException("TestingError"))
      .times(1)

    EasyMock.replay(path, s3Object, s3ClientMock)

    val uberdata = new IUberdataIO {
      override def copy(input: String, output: String): Unit = {
        copyFromS3(new URI(input), path, EasyMock.createMock(classOf[FileSystem]))
      }

      override lazy val s3Client = s3ClientMock
    }

    uberdata.copy(hdfspath, "outputFile")

    EasyMock.verify(path, s3Object, s3ClientMock)
  }

  it should "test copy method from S3 to hdfs logic" in {
    val path = EasyMock.createMock(classOf[Path])
    val mockFs = EasyMock.createMock(classOf[FileSystem])

    EasyMock.expect(path.getFileSystem(anyObject())).andReturn(mockFs).times(1)
    EasyMock
      .expect(mockFs.create(path))
      .andReturn(EasyMock.createMock(classOf[FSDataOutputStream]))
      .anyTimes()
    EasyMock.replay(path, mockFs)

    val uberdata = new IUberdataIO {
      override protected def createPathInstance(input: String) = path

      override protected def copyFromS3(input: URI,
                                        localPath: Path,
                                        localFileSystem: FileSystem): Unit = {
        assert(input == new URI(s3path))
        assert(path == localPath)
        assert(mockFs == localFileSystem)
      }
    }
    uberdata.copy(s3path, hdfspath)

    EasyMock.verify(path, mockFs)
  }

  it should "correct handle filesystem creation exception" in {
    val path = EasyMock.createMock(classOf[Path])
    val fs = EasyMock.createMock(classOf[FileSystem])
    val s3ClientMock = EasyMock.createMock(classOf[AmazonS3])

    EasyMock.expect(path.getFileSystem(anyObject())).andThrow(new IOException()).times(1)
    EasyMock.replay(path, fs, s3ClientMock)

    val uberdata = new IUberdataIO {
      override lazy val s3Client = s3ClientMock

      override protected def createPathInstance(input: String) = path
    }
    intercept[IOException] {
      uberdata.copy(hdfspath, s3path)
    }

    EasyMock.verify(path, fs, s3ClientMock)
  }

  it should "throw exception for unsuported protocol" in {
    val uberdata = new IUberdataIO
    intercept[NotImplementedError] {
      uberdata.copy(s3path, s3path)
    }
  }

}
