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

import eleflow.uberdata.core.data.UberDataset
import UberDataset._
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.core.enums.DateSplitType._
import eleflow.uberdata.core.exception.UnexpectedValueException
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{Suite, flatspec, matchers}
import eleflow.uberdata.enums.SupportedAlgorithm._
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.ml.ArimaModel
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should
import org.scalatest.matchers.should.Matchers.a

/**
 * Created by dirceu on 26/05/16.
 */
class TestForecastPredictor extends AnyFlatSpec with should.Matchers with BeforeAndAfterWithContext {
	this: Suite =>

	lazy val arimaData = List(
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
		Row(2d, 610d, 616),
		Row(2d, 513d, 150),
		Row(2d, 520d, 1050),
		Row(2d, 540d, 135),
		Row(2d, 560d, 358),
		Row(2d, 400d, 688),
		Row(2d, 450d, 2258),
		Row(2d, 590d, 3184),
		Row(2d, 1500d, 982),
		Row(2d, 5100d, 880),
		Row(2d, 800d, 800),
		Row(2d, 800d, 3518)
	)

	lazy val testArimaData = List(
		Row(1d, 1d, 1),
		Row(1d, 2d, 2),
		Row(1d, 3d, 3),
		Row(1d, 4d, 4),
		Row(1d, 5d, 5),
		Row(1d, 6d, 6),
		Row(1d, 7d, 7),
		Row(1d, 8d, 8),
		Row(1d, 9d, 9),
		Row(1d, 10d, 10),
		Row(1d, 11d, 11),
		Row(1d, 12d, 12),
		Row(1d, 13d, 13),
		Row(1d, 14d, 14),
		Row(1d, 15d, 15),
		Row(1d, 16d, 16),
		Row(2d, 17d, 17),
		Row(2d, 18d, 18),
		Row(2d, 19d, 19),
		Row(2d, 20d, 20),
		Row(2d, 21d, 21),
		Row(2d, 22d, 22),
		Row(2d, 23d, 23),
		Row(2d, 24d, 24),
		Row(2d, 25d, 25),
		Row(2d, 26d, 26),
		Row(2d, 27d, 27),
		Row(2d, 28d, 28),
		Row(2d, 29d, 29),
		Row(2d, 30d, 30),
		Row(2d, 31d, 31),
		Row(2d, 32d, 32)
	)

	val groupedArimaList = arimaData ++ List(
		Row(3d, 500d, 1900),
		Row(3d, 505d, 2880),
		Row(3d, 507d, 13000),
		Row(3d, 509d, 14500),
		Row(3d, 510d, 56516),
		Row(3d, 513d, 11650),
		Row(3d, 520d, 11050),
		Row(3d, 540d, 13835),
		Row(3d, 560d, 41358),
		Row(3d, 400d, 51688),
		Row(3d, 450d, 6258),
		Row(3d, 590d, 3784),
		Row(3d, 505d, 71982),
		Row(3d, 100d, 52880),
		Row(3d, 800d, 28000),
		Row(3d, 800d, 32518),
		Row(4d, 500d, 9100),
		Row(4d, 514d, 68280),
		Row(4d, 507d, 110000),
		Row(4d, 509d, 510500),
		Row(4d, 610d, 6616),
		Row(4d, 513d, 2150),
		Row(4d, 520d, 61050),
		Row(4d, 540d, 62135),
		Row(4d, 560d, 358),
		Row(4d, 400d, 688),
		Row(4d, 450d, 212258),
		Row(4d, 590d, 36184),
		Row(4d, 1500d, 4982),
		Row(4d, 5100d, 11880),
		Row(4d, 800d, 86600),
		Row(4d, 800d, 31518)
	)

	val groupedArimaTest = testArimaData ++ List(
		Row(3d, 35d, 41),
		Row(3d, 36d, 42),
		Row(3d, 37d, 43),
		Row(3d, 38d, 44),
		Row(3d, 39d, 45),
		Row(3d, 40d, 46),
		Row(3d, 41d, 47),
		Row(3d, 58d, 48),
		Row(3d, 59d, 49),
		Row(3d, 60d, 50),
		Row(3d, 61d, 51),
		Row(3d, 62d, 52),
		Row(3d, 63d, 53),
		Row(3d, 64d, 54),
		Row(3d, 65d, 55),
		Row(3d, 66d, 56),
		Row(4d, 67d, 57),
		Row(4d, 68d, 58),
		Row(4d, 69d, 59),
		Row(4d, 70d, 60),
		Row(4d, 71d, 61),
		Row(4d, 72d, 62),
		Row(4d, 73d, 63),
		Row(4d, 74d, 64),
		Row(4d, 75d, 65),
		Row(4d, 76d, 66),
		Row(4d, 77d, 67),
		Row(4d, 78d, 68),
		Row(4d, 79d, 69),
		Row(4d, 80d, 70),
		Row(4d, 81d, 71),
		Row(4d, 82d, 72))

	lazy val data = List(
		Row(1d, 500d, 900, true),
		Row(1d, 505d, 880, true),
		Row(1d, 507d, 1000, true),
		Row(1d, 509d, 1500, true),
		Row(1d, 510d, 6516, true),
		Row(1d, 513d, 1650, true),
		Row(1d, 520d, 1050, true),
		Row(1d, 540d, 1835, true),
		Row(1d, 560d, 1358, true),
		Row(1d, 400d, 1688, true),
		Row(1d, 450d, 0, false),
		Row(1d, 590d, 384, true),
		Row(1d, 505d, 1982, true),
		Row(1d, 100d, 2880, true),
		Row(1d, 800d, 8000, true),
		Row(1d, 800d, 3518, true),
		Row(2d, 500d, 900, true),
		Row(2d, 512d, 8280, true),
		Row(2d, 507d, 10000, true),
		Row(2d, 509d, 10500, true),
		Row(2d, 610d, 616, true),
		Row(2d, 513d, 0, false),
		Row(2d, 520d, 1050, true),
		Row(2d, 540d, 135, true),
		Row(2d, 560d, 358, true),
		Row(2d, 400d, 688, true),
		Row(2d, 450d, 2258, true),
		Row(2d, 590d, 3184, true),
		Row(2d, 1500d, 982, true),
		Row(2d, 5100d, 880, true),
		Row(2d, 800d, 800, true),
		Row(2d, 800d, 3518, true)
	)

	lazy val testData = List(
		Row(1d, 1d, 1, true),
		Row(1d, 2d, 2, true),
		Row(1d, 3d, 3, true),
		Row(1d, 4d, 4, true),
		Row(1d, 5d, 5, true),
		Row(1d, 6d, 6, true),
		Row(1d, 7d, 7, true),
		Row(1d, 8d, 8, true),
		Row(1d, 9d, 9, false),
		Row(1d, 10d, 10, true),
		Row(1d, 11d, 11, true),
		Row(1d, 12d, 12, true),
		Row(1d, 13d, 13, true),
		Row(1d, 14d, 14, true),
		Row(1d, 15d, 15, true),
		Row(1d, 16d, 16, true),
		Row(2d, 17d, 17, true),
		Row(2d, 18d, 18, true),
		Row(2d, 19d, 19, false),
		Row(2d, 20d, 20, true),
		Row(2d, 21d, 21, true),
		Row(2d, 22d, 22, true),
		Row(2d, 23d, 23, true),
		Row(2d, 24d, 24, true),
		Row(2d, 25d, 25, true),
		Row(2d, 26d, 26, true),
		Row(2d, 27d, 27, true),
		Row(2d, 28d, 28, false),
		Row(2d, 29d, 29, true),
		Row(2d, 30d, 30, true),
		Row(2d, 31d, 31, true),
		Row(2d, 32d, 32, true)
	)

	val groupedList = data ++ List(
		Row(3d, 500d, 1900, true),
		Row(3d, 505d, 2880, true),
		Row(3d, 507d, 13000, true),
		Row(3d, 509d, 14500, true),
		Row(3d, 510d, 56516, true),
		Row(3d, 513d, 11650, true),
		Row(3d, 520d, 11050, true),
		Row(3d, 540d, 13835, true),
		Row(3d, 560d, 41358, true),
		Row(3d, 400d, 51688, true),
		Row(3d, 450d, 6258, true),
		Row(3d, 590d, 0, false),
		Row(3d, 505d, 71982, true),
		Row(3d, 100d, 52880, true),
		Row(3d, 800d, 28000, true),
		Row(3d, 800d, 32518, true),
		Row(4d, 500d, 9100, true),
		Row(4d, 514d, 68280, true),
		Row(4d, 507d, 110000, true),
		Row(4d, 509d, 510500, true),
		Row(4d, 610d, 6616, true),
		Row(4d, 513d, 0, false),
		Row(4d, 520d, 61050, true),
		Row(4d, 540d, 62135, true),
		Row(4d, 560d, 0, false),
		Row(4d, 400d, 688, true),
		Row(4d, 450d, 212258, true),
		Row(4d, 590d, 36184, true),
		Row(4d, 1500d, 4982, true),
		Row(4d, 5100d, 11880, true),
		Row(4d, 800d, 86600, true),
		Row(4d, 800d, 31518, true)
	)

	val groupedTest = testData ++ List(
		Row(3d, 35d, 41, true),
		Row(3d, 36d, 42, true),
		Row(3d, 37d, 43, true),
		Row(3d, 38d, 44, true),
		Row(3d, 39d, 45, true),
		Row(3d, 40d, 46, true),
		Row(3d, 41d, 47, true),
		Row(3d, 58d, 48, true),
		Row(3d, 59d, 49, false),
		Row(3d, 60d, 50, true),
		Row(3d, 61d, 51, true),
		Row(3d, 62d, 52, true),
		Row(3d, 63d, 53, true),
		Row(3d, 64d, 54, true),
		Row(3d, 65d, 55, true),
		Row(3d, 66d, 56, false),
		Row(4d, 67d, 57, true),
		Row(4d, 68d, 58, true),
		Row(4d, 69d, 59, true),
		Row(4d, 70d, 60, true),
		Row(4d, 71d, 61, true),
		Row(4d, 72d, 62, true),
		Row(4d, 73d, 63, true),
		Row(4d, 74d, 64, true),
		Row(4d, 75d, 65, false),
		Row(4d, 76d, 66, true),
		Row(4d, 77d, 67, true),
		Row(4d, 78d, 68, true),
		Row(4d, 79d, 69, true),
		Row(4d, 80d, 70, true),
		Row(4d, 81d, 71, true),
		Row(4d, 82d, 72, true))

	"ForecastPredictor" should "execute mean average and return predictions" in {
		@transient val sc = context.sparkContext //    val rdd = sc.parallelizeext.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("store", DoubleType),
				StructField("Date", DoubleType),
				StructField("features", IntegerType),
				StructField("Open", BooleanType)))

		val rdd = sc.parallelize(data)
		val dataFrame = sqlContext.createDataFrame(rdd, structType)

		val timeSeriesBestModelFinder =
			ForecastPredictor().prepareMovingAveragePipeline[Double]("store", windowSize = 6)
		val model = timeSeriesBestModelFinder.fit(dataFrame)
		val df = model.transform(dataFrame)

		val first = df.first
		assert(first.getAs[org.apache.spark.ml.linalg.Vector](1).toArray.length == 11)
		assert(first.getAs[Iterable[Double]](2).toArray.length == 11)
	}

	it should "execute ARIMA and return predictions" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("feat", DoubleType),
				StructField("Date", DoubleType),
				StructField("label", IntegerType)))

		val rdd = sc.parallelize(arimaData)
		val dataFrame = sqlContext.createDataFrame(rdd, structType)

		val timeSeriesBestModelFinder =
			ForecastPredictor().prepareARIMAPipeline[Double](groupByCol = "feat", nFutures = 5)
		val model = timeSeriesBestModelFinder.fit(dataFrame)
		val df = model.transform(dataFrame)
		val first = df.first
		assert(first.getAs[org.apache.spark.ml.linalg.Vector]("validation").toArray.length == 5)
		assert(
			first.getAs[org.apache.spark.ml.linalg.Vector]("featuresPrediction").toArray.length ==
				16)
	}

	it should "execute ARIMA without standard field names and return predictions" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Sales", IntegerType)))

		val rdd = sc.parallelize(arimaData)
		val dataFrame = sqlContext.createDataFrame(rdd, structType).filter("Sales !=0")

		val timeSeriesBestModelFinder = ForecastPredictor().prepareARIMAPipeline[Double](
			groupByCol = "Store",
			labelCol = "Sales",
			timeCol = "data",
			nFutures = 5)
		val model = timeSeriesBestModelFinder.fit(dataFrame)
		val df = model.transform(dataFrame)
		val first = df.first
		assert(first.getAs[org.apache.spark.ml.linalg.Vector]("validation").toArray.length == 5)
		assert(
			first.getAs[org.apache.spark.ml.linalg.Vector]("featuresPrediction").toArray.length == 16)
	}

	it should "execute holtWinters and return predictions" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("Date", DoubleType),
				StructField("label", IntegerType),
				StructField("Open", BooleanType)))

		val rdd = sc.parallelize(data)
		val dataFrame = sqlContext.createDataFrame(rdd, structType)

		val timeSeriesBestModelFinder =
			ForecastPredictor().prepareHOLTWintersPipeline[Double](groupByCol = "Store", nFutures = 8)
		val model = timeSeriesBestModelFinder.fit(dataFrame)
		val df = model.transform(dataFrame)
		val first = df.first
		assert(first.getAs[DenseVector]("validation").toArray.length == 8)
		assert(first.getAs[org.apache.spark.ml.linalg.Vector]("features").toArray.length == 16)
	}

	it should "predict with ARIMA without standard field names and return predictions" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Sales", IntegerType)))
		val testStructType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Id", IntegerType)))
		val rdd = sc.parallelize(arimaData)
		val testRdd = sc.parallelize(testArimaData)
		val dataFrame = sqlContext.createDataFrame(rdd, structType)
		val testDataFrame = sqlContext.createDataFrame(testRdd, testStructType)
		val (timeSeriesBestModelFinder, model, accuracy) = ForecastPredictor().predict[Double, Double](
			dataFrame,
			testDataFrame,
			"",
			Seq("Sales"),
			"data",
			"Id",
			"Store",
			SupportedAlgorithm.Arima,
			5)
		val first = timeSeriesBestModelFinder.collect
		val arima = model.stages.last.asInstanceOf[ArimaModel[Int]]
		val bestArima = arima.models.sortBy(_._2._2.minBy(_.metricResult).metricResult).first()
		val min = bestArima._2._2.minBy(_.metricResult)
		assert(first.length == 10)
		assert(model.stages.last.isInstanceOf[ArimaModel[_]])
		assert(min.metricResult < 1.7d)
	}

	it should "choose the best model for each group" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Sales", IntegerType)))
		val rdd = sc.parallelize(groupedArimaList)
		val dataFrame = sqlContext.createDataFrame(rdd, structType).filter("Sales !=0")

		val pipeline = ForecastPredictor().prepareBestForecastPipeline[Int](
			"Store",
			"Sales",
			"validation",
			"data",
			5,
			Seq(8, 12, 16, 24, 26),
			(0 to 2).toArray)
		val model = pipeline.fit(dataFrame)
		val result = model.transform(dataFrame)
		assert(result.collect().length == 4)
		assert(result.select(IUberdataForecastUtil.ALGORITHM).distinct().count() > 1)
	}

	it should "choose the best model for each group in predict method" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Sales", IntegerType)))
		val rdd = sc.parallelize(groupedArimaList)
		val trainDf = sqlContext.createDataFrame(rdd, structType).filter("Sales !=0")
		val testStructType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Id", IntegerType)))
		val testRdd = sc.parallelize(groupedArimaTest)
		val testDf = sqlContext.createDataFrame(testRdd, testStructType)

		val (result, _, accuracy) = ForecastPredictor().predict[Double, Double](
			trainDf,
			testDf,
			"Store",
			Seq("Sales"),
			"data",
			"Id",
			"Store",
			SupportedAlgorithm.FindBestForecast,
			5,
			Seq(8, 12, 16, 24, 26))

		assert(result.collect().length == 20)
		assert(result.rdd.map(_.getAs[String](IUberdataForecastUtil.ALGORITHM)).distinct().count() > 1)
	}

	"XGBoost" should "execute a prediction with a simple dataset" in {
		@transient val sc = context.sparkContext
		@transient val sqlContext = context.sqlContext

		val structType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Sales", IntegerType),
				StructField("Open", BooleanType)))
		val rdd = sc.parallelize(groupedList)
		val trainDf = sqlContext.createDataFrame(rdd, structType)
		val trainDfDouble = trainDf.withColumn("Sales", trainDf("Sales").cast(DoubleType))

		val testStructType = StructType(
			Seq(
				StructField("Store", DoubleType),
				StructField("data", DoubleType),
				StructField("Id", IntegerType),
				StructField("Open", BooleanType)))
		val testRdd = sc.parallelize(groupedTest)
		val testDf = sqlContext.createDataFrame(testRdd, testStructType)
		val (result, _, accuracy) = ForecastPredictor().predictSmallModelFeatureBased[Double, Int](
			trainDfDouble,
			testDf,
			"Sales",
			Seq("Store"),
			"data",
			"Id",
			"Store",
			SupportedAlgorithm.XGBoostAlgorithm,
			"validationCol")

		assert(result.collect().length == 64)
	}

	it should "execute prediction with Rossmann dataset" in {
		val train = UberDataset(context, s"$defaultFilePath/data/RossmannTrain.csv")
		train.applyColumnTypes(
			Seq(
				DoubleType,
				DoubleType,
				TimestampType,
				IntegerType,
				DoubleType,
				DoubleType,
				DoubleType,
				StringType,
				StringType))
		val trainData = train
			.formatDateValues("Date", DayMonthYear)
			.select(
				"Store",
				"Sales",
				"DayOfWeek",
				"Date1",
				"Date2",
				"Date3",
				"Open",
				"Promo",
				"StateHoliday",
				"SchoolHoliday")
			.cache
		val test = UberDataset(context, s"$defaultFilePath/data/RossmannTest.csv")
		test.applyColumnTypes(
			Seq(
				LongType,
				DoubleType,
				DoubleType,
				TimestampType,
				DoubleType,
				DoubleType,
				StringType,
				StringType))
		val testData = test.formatDateValues("Date", DayMonthYear)

		val trainSchema = trainData.schema
		val testSchema = testData.schema
		val sqlContext = context.sqlContext
		val convertedTest = sqlContext.createDataFrame(testData.rdd.map { row =>
			val seq = row.toSeq
			val newSeq = if (seq.contains(null)) {
				if (row.getAs[String]("StateHoliday") == "1.0")
					seq.updated(6, 0d)
				else seq.updated(6, 1d)
			} else seq
			Row(newSeq: _*)
		}, testSchema)
		val convertedTrain = sqlContext.createDataFrame(trainData.rdd.map { row =>
			val seq = row.toSeq
			val newSeq = if (seq.contains(null)) {
				if (row.getAs[String]("StateHoliday") == "1.0")
					seq.updated(6, 0d)
				else seq.updated(6, 1d)
			} else seq
			Row(newSeq: _*)
		}, trainSchema)
		val (bestDf, _, accuracy) = eleflow.uberdata
			.ForecastPredictor()
			.predictSmallModelFeatureBased[Int, Double](
			convertedTrain,
			convertedTest,
			"Sales",
			Seq("Store"),
			"Date1",
			"Id",
			"Store",
			XGBoostAlgorithm,
			"validacaocoluna")

		val cachedDf = bestDf.cache

		assert(cachedDf.count == 288)
		assert(train.count == 9420)
		assert(test.count == 288)
	}

	it should "accept different kind of data into it's columns " in {
		val train = UberDataset(context, s"$defaultFilePath/data/RossmannTrain.csv")
		train.applyColumnTypes(
			Seq(
				ShortType,
				ByteType,
				TimestampType,
				IntegerType,
				DoubleType,
				BooleanType,
				BooleanType,
				StringType,
				StringType))
		val trainData = train
			.formatDateValues("Date", DayMonthYear)
			.select(
				"Store",
				"Sales",
				"DayOfWeek",
				"Date1",
				"Date2",
				"Date3",
				"Open",
				"Promo",
				"StateHoliday",
				"SchoolHoliday")
			.cache
		val test = UberDataset(context, s"$defaultFilePath/data/RossmannTest.csv")
		test.applyColumnTypes(
			Seq(
				DoubleType,
				ShortType,
				ByteType,
				TimestampType,
				DoubleType,
				DoubleType,
				StringType,
				StringType))
		val testData = test.formatDateValues("Date", DayMonthYear)

		val trainSchema = trainData.schema
		val testSchema = testData.schema
		val sqlContext = context.sqlContext
		val convertedTest = sqlContext.createDataFrame(testData.rdd.map { row =>
			val seq = row.toSeq
			val newSeq = if (seq.contains(null)) {
				if (row.getAs[String]("StateHoliday") == "1.0") {
					seq.updated(6, 0d)
				} else {
					seq.updated(6, 1d)
				}
			} else {
				seq
			}
			Row(newSeq: _*)
		}, testSchema)
		val convertedTrain = sqlContext.createDataFrame(trainData.rdd.map { row =>
			val seq = row.toSeq
			val newSeq = if (seq.contains(null)) {
				if (row.getAs[String]("StateHoliday") == "1.0") {
					seq.updated(6, 0d)
				} else {
					seq.updated(6, 1d)
				}
			} else {
				seq
			}
			Row(newSeq: _*)
		}, trainSchema)
		val (bestDf, _, accuracy) = eleflow.uberdata
			.ForecastPredictor()
			.predictSmallModelFeatureBased[Int, Short](
			convertedTrain,
			convertedTest,
			"Sales",
			Seq("DayOfWeek", "Date2", "Date3", "Open", "Promo", "StateHoliday", "SchoolHoliday"),
			"Date1",
			"Id",
			"Store",
			XGBoostAlgorithm,
			"validacaocoluna")

		val cachedDf = bestDf.cache

		assert(cachedDf.count == 288)
		assert(train.count == 9420)
		assert(test.count == 288)
	}
	it should "throw UnsupportedOperationException when the algorithm is invalid" in {
		a[UnexpectedValueException] should be thrownBy {
			val dataframe = context.sqlContext.emptyDataFrame
			eleflow.uberdata
				.ForecastPredictor()
				.predict[Double, Double](
				dataframe,
				dataframe,
				"",
				Seq("Date"),
				"",
				"",
				"",
				SupportedAlgorithm.LinearLeastSquares)
		}
	}

	it should "throw IllegalArgumentexception when the algorithm featurescol is empty" in {
		a[IllegalArgumentException] should be thrownBy {
			val dataframe = context.sqlContext.emptyDataFrame
			eleflow.uberdata
				.ForecastPredictor()
				.predict[Double, Double](
				dataframe,
				dataframe,
				"",
				Seq.empty[String],
				"",
				"",
				"",
				SupportedAlgorithm.LinearLeastSquares)
		}
	}

}
