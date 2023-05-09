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

package eleflow.uberdata.data

import eleflow.uberdata.core.util.ClusterSettings
import eleflow.uberdata.core.data.UberDataset._
import eleflow.uberdata.core.data.{DataTransformer, UberDataset}
import eleflow.uberdata.core.enums.DataSetType
import eleflow.uberdata.core.util.DateTimeParser
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.scalatest._

/**
  * Created by dirceu on 21/10/14.
  */
class TestDataTransformer extends FunSuite with Matchers with BeforeAndAfterWithContext {
  this: Suite =>

  test("correctly load data with empty columns at the end") {
    val testDataSet = UberDataset(context, s"${defaultFilePath}LoadRddDataTransformerTestData.csv")
    val dataset = UberDataset(context, s"${defaultFilePath}LoadRddDataTransformerData.csv")
    val (result, _, _) =
      DataTransformer.createLabeledPointFromRDD(dataset, testDataSet, "t1", "int")
    val all = result.take(3)
    val (_, first) = all.head
    val (_, second) = all.tail.head

    // format: off
    assert(first.label == 3)
    assert(first.features.toArray.deep == Array[Double](0.0, 1.0, 0.0, 0.0, 0.0, 10.5, 0.0, 1.0, 0.0).deep)
    assert(second.label == 4)
    assert(second.features.toArray.deep == Array[Double](1.0, 0.0, 0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 1.0).deep)
    // format: on
  }

  test("correct enforce double as bigdecimal ") {
    ClusterSettings.enforceDoubleAsBigDecimal = true
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext
    val dataset = UberDataset(context, s"${defaultFilePath}LoadRddDataTransformerData.csv")

    def createBigDecimal(value: Double) =
      new java.math.BigDecimal(value.toString).setScale(ClusterSettings.defaultDecimalScale)
    val result = dataset.toDataFrame
    assert(
      result.collect.deep == Array(
        new GenericRow(Array[Any](3l, 5l, "vlr1", createBigDecimal(10.5), "va")),
        new GenericRow(Array[Any](4l, 1l, "vl3", createBigDecimal(0.1), "vb")),
        new GenericRow(Array[Any](5l, 8l, "vlr1", createBigDecimal(10.0), ""))).deep)
  }

  test("Correct handle date values") {
    val dataSet =
      UberDataset(context, s"${defaultFilePath}HandleDataTransformer.csv", DateTimeParser(0))
    val results = DataTransformer
      .createLabeledPointFromRDD(dataSet, Seq("id"), Seq(), DataSetType.Test)
      .take(3)
    // format: off
    assert(results(0)._2.features.toArray.deep == Array(5.0, 0.0, 1.0, 10.5, 394296.0).deep)
    assert(results(1)._2.features.toArray.deep == Array(1.0, 1.0, 0.0, 0.1, 394176.0).deep)
    assert(results(2)._2.features.toArray.deep == Array(8.0, 1.0, 0.0, 10.0, 394176.0).deep)
    // format: on
  }

  test("create labeledpoint for multiplestrings") {
    val train = UberDataset(context, s"${defaultFilePath}MultipleStringsTest.csv")
    train.applyColumnTypes(
      Seq(
        LongType,
        StringType,
        StringType,
        DecimalType(ClusterSettings.defaultDecimalPrecision, ClusterSettings.defaultDecimalScale),
        StringType,
        StringType,
        LongType,
        StringType))
    val labeledPoint =
      DataTransformer.createLabeledPointFromRDD(train, Seq("int"), Seq(), DataSetType.Test)
    val result = labeledPoint.take(3)
    // format: off
    assert(result.head._1._1 == 5.0)
    assert(result.head._1._2 == 5)
    assert(result.head._2.label == 5.0)
    assert(result.head._2.features.size == 14)
    assert(result.head._2.features.toArray.deep == Array(0.0, 1.0, 1.0, 0.0, 10.5, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0).deep)
    assert(result(1)._1._1 == 1.0)
    assert(result(1)._1._2 == 1)
    assert(result(1)._2.label == 1.0)
    assert(result(1)._2.features.toArray.deep == Array(1.0, 0.0, 1.0, 0.0, 0.1, 1.0, 0.0, 0.0, 1.0, 0.0, 2.0, 0.0, 1.0, 0.0).deep)
    assert(result(2)._1._1 == 8.0)
    assert(result(2)._1._2 == 8)
    assert(result(2)._2.label == 8.0)
    assert(result(2)._2.features.toArray.deep == Array(0.0, 1.0, 0.0, 1.0, 10.0, 0.0, 0.0, 1.0, 0.0, 1.0, 3.0, 1.0, 0.0, 0.0).deep)
    // format: on
  }
}
