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

import org.apache.spark.ml.{ArimaModel, HoltWintersModel}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers, Suite}

/**
  * Created by dirceu on 02/05/16.
  */
class TestTimeSeriesBestModelFinder extends FlatSpec with Matchers with BeforeAndAfterWithContext {
  this: Suite =>

  lazy val ts = Vectors.dense(Array(9007d, 8106d, 8928d, 9137d, 10017d, 10826d, 11317d, 10744d, 9713d, 9938d, 9161d,
    8927d, 7750d, 6981d, 8038d, 8422d, 8714d, 9512d, 10120d, 9823d, 8743d, 9129d, 8710d, 8680d, 8162d,
    7306d, 8124d, 7870d, 9387d, 9556d, 10093d, 9620d, 8285d, 8466d, 8160d, 8034d, 7717d, 7461d, 7767d,
    7925d, 8623d, 8945d, 10078d, 9179d, 8037d, 8488d, 7874d, 8647d, 7792d, 6957d, 7726d, 8106d, 8890d,
    9299d, 10625d, 9302d, 8314d, 8850d, 8265d, 8796d, 7836d, 6892d, 7791d, 8192d, 9115d, 9434d, 10484d,
    9827d, 9110d, 9070d, 8633d, 9240d))
  lazy val ts2 = Vectors.dense(Array(9007d, 18106d, 928d, 59137d, 90017d, 826d, 211317d, 744d, 13d, 89938d, 109161d,
    27d, 750d, 566981d, 58038d, 78422d, 48714d, 512d, 110120d, 59823d, 98743d, 29129d, 10d, 80d, 88162d,
    97306d, 88124d, 107870d, 387d, 96556d, 510093d, 20d, 98285d, 88466d, 108160d, 34d, 717d, 461d, 767d,
    97925d, 48623d, 458945d, 170078d, 59179d, 98037d, 488d, 57874d, 68647d, 37792d, 76957d, 726d, 88106d,
    108890d, 89299d, 610625d, 29302d, 14d, 58850d, 88265d, 796d, 67836d, 16892d, 57791d, 98192d, 15d, 99434d,
    84d, 79827d, 59110d, 39070d, 633d, 69240d))

  lazy val groupedData = List(
    Row(1d, ts),
    Row(2d, ts2))

  lazy val data = List(
    Row(1d, 500d, 900),
    Row(1d, 505d, 880),
    Row(1d, 507d, 1000),
    Row(1d, 509d, 1500),
    Row(1d, 510d, 6516),
    Row(1d, 513d, 1650),
    Row(1d, 520d, 1050),
    Row(1d, 540d, 1835),
    Row(1d, 560d, 1358),
    Row(1d, 400d, 1688),
    Row(1d, 450d, 258),
    Row(1d, 590d, 384),
    Row(1d, 505d, 1982),
    Row(1d, 100d, 2880),
    Row(1d, 800d, 8000),
    Row(1d, 800d, 3518),
    Row(2d, 500d, 900),
    Row(2d, 512d, 8280),
    Row(2d, 507d, 10000),
    Row(2d, 509d, 10500),
    Row(2d, 610d, 6160),
    Row(2d, 513d, 5500),
    Row(2d, 520d, 10050),
    Row(2d, 540d, 1305),
    Row(2d, 560d, 3058),
    Row(2d, 400d, 6088),
    Row(2d, 450d, 2258),
    Row(2d, 590d, 3184),
    Row(2d, 1500d, 9802),
    Row(2d, 5100d, 8080),
    Row(2d, 800d, 8010),
    Row(2d, 800d, 3518)
  )
  "TimeSeriesBestModelfinder" should
    "model execution best value should return an root squared percentage error bellow 50%" in {

    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("label", DoubleType), StructField("Date", DoubleType),
      StructField("features", IntegerType)))

    val rdd = sc.parallelize(data)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().prepareARIMAPipeline[Double](nFutures = 6)
    val model = timeSeriesBestModelFinder.fit(dataFrame)

    val head = model.stages.last
    assert(head.isInstanceOf[ArimaModel[_]])
    assert(head.asInstanceOf[ArimaModel[Double]].models.map(_._2._2.map(_.metricResult).min).min() < 1.05)
  }

  it should "receive input columns with names different from features" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("label", DoubleType, nullable = false),
      StructField("sales", new VectorUDT, nullable = false)))



    val rdd = sc.parallelize(groupedData)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().prepareARIMAPipeline[Double](labelCol = "sale",
      featuresCol = "sales", nFutures = 6)
    intercept[IllegalArgumentException] {
      timeSeriesBestModelFinder.fit(dataFrame)
    }

  }
  it should "receive input columns with names different from label" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("store", DoubleType, nullable = false),
      StructField("sales", new VectorUDT, nullable = false)))



    val rdd = sc.parallelize(groupedData)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().prepareARIMAPipeline[Double](labelCol = "store",
      featuresCol = "sales", nFutures = 6)
    intercept[IllegalArgumentException] {
      timeSeriesBestModelFinder.fit(dataFrame)
    }


  }

  it should "receive input columns with names different from label in HoltWinters" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("store", DoubleType, nullable = false),
      StructField("sales", new VectorUDT, nullable = false)))



    val rdd = sc.parallelize(groupedData)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().prepareHOLTWintersPipeline[Double](labelCol = "store",
      featuresCol = "sales", nFutures = 6)
    intercept[IllegalArgumentException] {
      timeSeriesBestModelFinder.fit(dataFrame)
    }
  }

  it should "model execution best value should return an root squared percentage error bellow 50% in HoltWinters" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("label", DoubleType), StructField("Date", DoubleType), StructField("features", IntegerType)))

    val rdd = sc.parallelize(data)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().prepareHOLTWintersPipeline[Double](nFutures = 6)
    val model = timeSeriesBestModelFinder.fit(dataFrame)

    val head = model.stages.last
    assert(head.isInstanceOf[HoltWintersModel[_]])
    assert(head.asInstanceOf[HoltWintersModel[Double]].models.map(_._2._2.metricResult).min() < 2)
  }
}
