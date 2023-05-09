package eleflow.uberdata.core

import eleflow.uberdata.core.data.UberDataset
import org.apache.spark.rpc.netty.{BeforeAndAfterWithContext, TestSparkConf}
import org.scalatest._
import flatspec._
import matchers._

/**
 * Created by dirceu on 26/02/15.
 */
class TestFuncUberdataContext extends AnyFlatSpec with should.Matchers with BeforeAndAfterWithContext {
  this: Suite =>

  class LocalContextI extends IUberdataContext(TestSparkConf.conf) {

    def extractFileName(file: String) = {
      val name = file.split("/").last
      val index = name.indexOf(".csv") + name.indexOf(".txt")
      name.splitAt(index + 1).productIterator.toList.filter(!_.toString.isEmpty).head.toString
    }
  }

  override val defaultFilePath = "sparknotebook/src/test/resources/"

  "FuncUberdataContext " should "extract .csv from the file name" in {

    val context = new LocalContextI

    assert(context.extractFileName("arquivo.csv") == "arquivo")
    assert(context.extractFileName("/home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("/home/subfolder/arquivo.csv") == "arquivo")
    assert(context.extractFileName("//home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("//home/subfolder/arquivo.csv") == "arquivo")
    assert(context.extractFileName("///home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("///home/subfolder/arquivo.csv") == "arquivo")
    assert(context.extractFileName("s3://home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("s3://home/subfolder/arquivo.csv") == "arquivo")
    assert(context.extractFileName("hdfs://home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("hdfs://home/subfolder/arquivo.csv") == "arquivo")
    assert(context.extractFileName("s3:///home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("s3:///home/subfolder/arquivo.csv") == "arquivo")
    assert(context.extractFileName("hdfs:///home/arquivo.csv") == "arquivo")
    assert(context.extractFileName("hdfs:///home/subfolder/arquivo.csv") == "arquivo")
  }

  it should "extract .txt from the file name" in {

    val context = new LocalContextI

    assert(context.extractFileName("arquivo.txt") == "arquivo")
    assert(context.extractFileName("/home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("/home/subfolder/arquivo.txt") == "arquivo")
    assert(context.extractFileName("//home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("//home/subfolder/arquivo.txt") == "arquivo")
    assert(context.extractFileName("///home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("///home/subfolder/arquivo.txt") == "arquivo")
    assert(context.extractFileName("s3://home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("s3://home/subfolder/arquivo.txt") == "arquivo")
    assert(context.extractFileName("hdfs://home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("hdfs://home/subfolder/arquivo.txt") == "arquivo")
    assert(context.extractFileName("s3:///home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("s3:///home/subfolder/arquivo.txt") == "arquivo")
    assert(context.extractFileName("hdfs:///home/arquivo.txt") == "arquivo")
    assert(context.extractFileName("hdfs:///home/subfolder/arquivo.txt") == "arquivo")
  }
}
