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

package eleflow.uberdata.data.stat

import eleflow.uberdata.core.util.ClusterSettings
import eleflow.uberdata.core.data.UberDataset
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util.Objects

/**
  * Created by dirceu on 30/12/14.
  */
class TestStatistics extends AnyFlatSpec with should.Matchers with BeforeAndAfterWithContext {

  lazy val schema = StructType(
    Array(
      StructField("click", IntegerType, nullable = false),
      StructField("c1", IntegerType, nullable = true),
      StructField("c2", IntegerType, nullable = true),
      StructField("c3", IntegerType, nullable = true),
      StructField("c4", IntegerType, nullable = true),
      StructField("c5", IntegerType, nullable = true),
      StructField("c6", IntegerType, nullable = true),
      StructField("c7", IntegerType, nullable = true)))

  def buildDataset = context.sparkContext.parallelize(values)

  import UberDataset._

  lazy val correlatedValues = Seq(
    //= [c1: int, c2: int]
    Row(1, 3),
    Row(2, 6),
    Row(3, 9),
    Row(4, 12),
    Row(5, 15),
    Row(6, 18),
    Row(7, 21),
    Row(8, 24),
    Row(9, 27),
    Row(10, 30)
  )

  lazy val values = Seq(
    Row(0, 0, 0, 0, 0, 0, 351, 0),
    Row(0, 0, 0, 0, 0, 0, 3251, 0),
    Row(1, 1, 0, 0, 0, 0, 5351, 0),
    Row(1, 1, 0, 0, 0, 0, 851, 0),
    Row(1, 0, 0, 0, 0, 0, 6651, 0),
    Row(0, 0, 0, 0, 0, 0, 451, 0),
    Row(0, 0, 0, 0, 0, 0, 1, 0),
    Row(1, 0, 0, 1, 0, 0, 21, 0),
    Row(0, 0, 0, 0, 0, 0, 32351, 0),
    Row(0, 0, 0, 0, 0, 0, 4551, 0)
  )

  val negativeCorrelationValues = Seq(
    Row(1, -3, 2),
    Row(2, -6, 4),
    Row(3, -9, 6),
    Row(4, -12, 7),
    Row(5, -15, 10),
    Row(6, -18, 11),
    Row(7, -21, 14),
    Row(8, -24, 16),
    Row(9, -27, 18),
    Row(10, -30, 20)
  )

  val rawValues = Seq(
    LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0, 351, 0)),
    LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0, 3251, 0)),
    LabeledPoint(1, Vectors.dense(0, 1, 1, 0, 0, 5351, 0)),
    LabeledPoint(1, Vectors.dense(0, 1, 1, 0, 0, 851, 0)),
    LabeledPoint(1, Vectors.dense(0, 1, 1, 0, 0, 6651, 0)),
    LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0, 451, 0)),
    LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0, 1, 0)),
    LabeledPoint(1, Vectors.dense(0, 1, 1, 0, 0, 21, 0)),
    LabeledPoint(0, Vectors.dense(0, 0, 0, 0, 0, 32351, 0)),
    LabeledPoint(0, Vectors.dense(0, 0, 1, 0, 0, 4551, 0))
  )
  "Correlation" should "return the matrix of column correlations" in {
    val rdd = context.sqlContext.createDataFrame(buildDataset, schema)
    val result = Statistics.correlation(rdd)
    assert(result.toArray.count(f => !f._1.isNaN && f._1 != 1d) == 6)
    assert(roundDouble(result.head._1, 2) == 0.61)
    assert(roundDouble(result(1)._1, 2) == 0.41)
    assert(roundDouble(result(2)._1, 3) == -0.193)
    assert(roundDouble(result(3)._1, 3) == -0.190)
    assert(roundDouble(result(4)._1, 2) == -0.17)
    assert(roundDouble(result(5)._1, 2) == -0.12)
    assert(result.head._2 == "(c1,) | (click,)")
    assert(result(1)._2 == "(c3,) | (click,)")
    assert(result(2)._2 == "(c6,) | (c3,)")
    assert(result(3)._2 == "(c6,) | (click,)")
    assert(result(4)._2 == "(c3,) | (c1,)")
    assert(result(5)._2 == "(c6,) | (c1,)")

  }

  it should "return 1 for two totally correlated columns" in {
    val _dataset = context.sparkContext.parallelize(correlatedValues)
    val _schema = StructType(
      Array(
        StructField("c1", IntegerType, nullable = true),
        StructField("c2", IntegerType, nullable = true)
      ))

    val rdd = context.sqlContext.createDataFrame(_dataset, _schema)
    val result = Statistics.correlation(rdd, 3)
    assert(result.size == 1)
    assert(result.forall(!_._1.isNaN))
    assert(roundDouble(result.head._1, 2) == 1.00)
    assert(result.head._2 == "(c2,) | (c1,)")
  }

  it should "return negative correlations sorted by the absolute value" in {
    val _dataset = context.sparkContext.parallelize(negativeCorrelationValues)
    val _schema = StructType(
      Array(
        StructField("c1", IntegerType, nullable = true),
        StructField("c2", IntegerType, nullable = true),
        StructField("c3", IntegerType, nullable = true)
      ))

    val rdd = context.sqlContext.createDataFrame(_dataset, _schema)
    val result = Statistics.correlation(rdd, 5)
    assert(result.size == 3)
    assert(result.forall(!_._1.isNaN))
    assert(roundDouble(result.head._1, 2) == -1.00)
    assert(result.head._2 == "(c2,) | (c1,)")
    assert(roundDouble(result(1)._1, 4) == -0.9976)
    assert(result(1)._2 == "(c3,) | (c2,)")
    assert(roundDouble(result(2)._1, 4) == 0.9976)
    assert(result(2)._2 == "(c3,) | (c1,)")
  }

  it should "return the distinct values of column correlations" in {
    val rdd = context.sqlContext.createDataFrame(buildDataset, schema)
    val result = Statistics.correlation(rdd, 3)
    assert(result.size == 3)
    assert(result.forall(!_._1.isNaN))
    val resumedResult = Statistics.correlation(rdd, 2)
    assert(resumedResult.size == 2)
  }

  import UberDataset._

  it should "return the matrix of column correlations for dataset" in {

    val dataSet = UberDataset(context, s"${defaultFilePath}CorrelationDataSet.csv")
    dataSet.applyColumnTypes(
      Seq(
        StringType,
        StringType,
        DecimalType(ClusterSettings.defaultDecimalPrecision, ClusterSettings.defaultDecimalScale),
        StringType,
        StringType,
        LongType,
        StringType))
    val resultWithSizeLimit = Statistics.correlation(dataSet).toArray
    assert(resultWithSizeLimit.count(f => !f._1.isNaN) == 20)
    assert(resultWithSizeLimit(0)._2 == "(string,vl3) | (string2,vl3)")
    assert(resultWithSizeLimit(1)._2 == "(string5,025st) | (string3,str05)")

    val resultWithAllColumnCorrelations = Statistics.correlation(dataSet, 400).toArray
    assert(resultWithAllColumnCorrelations.count(f => !f._1.isNaN) == 273)
    assert(resultWithAllColumnCorrelations(19)._2 == "(double,) | (string2,vlr1)")
    assert(roundDouble(resultWithAllColumnCorrelations(19)._1, 2) == 0.85)

    assert(resultWithAllColumnCorrelations(20)._2 == "(string3,str05) | (double,)")
    assert(roundDouble(resultWithAllColumnCorrelations(20)._1, 2) == 0.72)
  }

  "Target Correlation" should "return the distinct values of column correlations with Target" in {
    val rdd = context.sqlContext.createDataFrame(buildDataset, schema)
    val result = Statistics.correlation(rdd, 3)
    assert(result.size == 3)
    assert(result.forall(!_._1.isNaN))
    val resumedResult = Statistics.targetCorrelation(rdd, 2, Seq("id"))
    assert(resumedResult.size == 2)
    assert(!resumedResult.contains((1, _: Double)))
  }

  "Labeledpoint with correlations" should "transform the labeledpoint retaining only the correlated columns with limited number" in {
    val rdd = context.sqlContext.createDataFrame(buildDataset, schema)
    val result = Statistics.correlation(rdd, 3)
    assert(result.size == 3)
    assert(result.forall(!_._1.isNaN))
    val data = context.sparkContext.parallelize(rawValues)
    val (resumedResult, _, _, _) = Statistics.correlationLabeledPoint(data, data, data, Left(20))
    assert(resumedResult.count == 10)
//    assert(resumedResult.first.features.toArray.deep == Array(0.0, 0.0, 351.0).deep)
    assert(Objects.deepEquals(resumedResult.first.features.toArray, Array(0.0, 0.0, 351.0)))
  }
  it should "transform the labeledpoint retaining only the correlated columns with limited correlation" in {
    val rdd = context.sqlContext.createDataFrame(buildDataset, schema)
    val result = Statistics.correlation(rdd, 3)
    assert(result.size == 3)
    assert(result.forall(!_._1.isNaN))
    val data = context.sparkContext.parallelize(rawValues)
    val (resumedResult, _, _, _) = Statistics.correlationLabeledPoint(data, data, data, Right(.19))
    assert(resumedResult.count == 10)
//    assert(resumedResult.first.features.toArray.deep == Array(0.0, 0.0, 351.0).deep)
    assert(Objects.deepEquals(resumedResult.first.features.toArray, Array(0.0, 0.0, 351.0)))
  }

  private def roundDouble(number: Double, scale: Int) = {
    BigDecimal(number).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
