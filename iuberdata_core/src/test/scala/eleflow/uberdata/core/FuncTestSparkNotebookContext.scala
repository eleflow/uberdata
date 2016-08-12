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

import eleflow.uberdata.core.data.{DataTransformer, Dataset}
import eleflow.uberdata.core.enums.DataSetType
import org.apache.spark.rpc.netty.{BeforeAndAfterWithContext, TestSparkConf}
import org.apache.spark.SparkException
import org.scalatest._

/**
 * Created by dirceu on 14/10/14.
  *
 */
class FuncTestSparkNotebookContext extends FlatSpec with BeforeAndAfterWithContext {
  this: Suite =>

  val uberContext = context

  "Functional SparkNotebookContext" should
    "correctly load rdd" in {

    import Dataset._

    val dataset = Dataset(uberContext, s"${defaultFilePath}FuncTestSparkNotebookContextFile1.csv")

    val testDataSet = Dataset(uberContext, s"${defaultFilePath}FuncTestSparkNotebookContextFile2.csv")


    val (train,test,_) = DataTransformer.createLabeledPointFromRDD(dataset, testDataSet, "int","id")
    val all = train.take(3)
    val (_, first) = all.head
    val (_, second) = all.tail.head
    assert(first.label == 5.0)
    assert(first.features.toArray.deep == Array[Double](0.0, 1.0, 10.5).deep)
    assert(second.label == 1.0)
    assert(second.features.toArray.deep == Array[Double](1.0, 0.0, 0.1).deep)

    val allTest = test.take(3)
    val (_, firstTest) = allTest.head
    val (_, secondTest) = allTest.tail.head
    assert(firstTest.label == 1.0)
    assert(firstTest.features.toArray.deep == Array[Double](0.0, 1.0, 10.5).deep)
    assert(secondTest.label == 2.0)
    assert(secondTest.features.toArray.deep == Array[Double](1.0, 0.0, 0.1).deep)
  }

  it should "Throw an exception when process an empty numeric column" in {

    @transient lazy val context = uberContext

    context.sparkContext
    try {
      import Dataset._
      val dataset = Dataset(context, s"${defaultFilePath}FuncTestSparkNotebookContextFile1.csv")
      dataset.take(3)
    } catch {
      case e: SparkException =>
        assert(e.getMessage.contains("UnexpectedFileFormatException"))
    }
  }

  it should "Correct handle empty string values" in {
    @transient lazy val context = uberContext
    context.sparkContext
    val schemaRdd = Dataset(context, s"${defaultFilePath}FuncTestSparkNotebookContextEmpty.csv").toDataFrame
    val result = DataTransformer.createLabeledPointFromRDD(schemaRdd, Seq("int"), Seq("id"), DataSetType.Train)
    assert(result.count() == 3)
  }

  it should "Throw an exception when input have different number of columns" in {
    uberContext.sparkContext
    try {

      context.load(s"${defaultFilePath}FuncTestSparkNotebookContextFile1.csv", TestSparkConf.separator)
    } catch {
      case e: SparkException =>
        assert(e.getMessage.contains("UnexpectedFileFormatException"))
    }
  }

}
