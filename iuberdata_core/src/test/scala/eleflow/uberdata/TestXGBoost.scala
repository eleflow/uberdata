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

package eleflow.uberdata

import com.cloudera.sparkts.models.UberXGBoostModel
import eleflow.uberdata.core.data.Dataset
import eleflow.uberdata.core.data.Dataset._
import eleflow.uberdata.core.enums.DateSplitType._
import eleflow.uberdata.core.util.ClusterSettings
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.HasXGBoostParams
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import ml.dmlc.xgboost4j.{LabeledPoint => XGBLabeledPoint}
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}
import org.apache.spark.util.random
import org.scalatest.{FlatSpec, Matchers, Suite}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by dirceu on 25/08/16.
  */
class TestXGBoost
    extends FlatSpec
    with Matchers
    with BeforeAndAfterWithContext
    with HasXGBoostParams { this: Suite =>

  ClusterSettings.xgBoostWorkers = 1

  "XGBostAlgorithm" should "execute xgboost " in {
		val train = Dataset(context, s"$defaultFilePath/data/RossmannTrain.csv")

		val trainData = train.formatDateValues("Date", DayMonthYear).select("Store", "Sales",
			"DayOfWeek", "Date1", "Date2", "Date3", "Open", "Promo", "StateHoliday", "SchoolHoliday")
			.cache
		val test = Dataset(context, s"$defaultFilePath/data/RossmannTest.csv")

		val testData = test.formatDateValues("Date", DayMonthYear).map{
		row =>
			XGBLabeledPoint.fromDenseVector(row.getAs[Long]("Id"), Array(row.getAs[Long]("Store").toFloat,
				row.getAs[Long]("DayOfWeek").toFloat, row.getAs[Int]("Date1").toFloat,
				row.getAs[Int]("Date2").toFloat,row.getAs[Int]("Date3").toFloat))
		}

		val trainLabel = trainData.map{
			row =>
				LabeledPoint(row.getAs[Long]("Sales").toDouble,
					Vectors.dense(Array(row.getAs[Long]("Store").toDouble,
						row.getAs[Long]("DayOfWeek").toDouble, row.getAs[Int]("Date1").toDouble,
						row.getAs[Int]("Date2").toDouble, row.getAs[Int]("Date3").toDouble)))
		}

		val model = UberXGBoostModel.train(trainLabel, Map[String, Any](
			"silent" -> 1
			, "objective" -> "reg:linear"
			, "booster" -> "gbtree"
			, "eta" -> 0.0225
			, "max_depth" -> 26
			, "subsample" -> 0.63
			, "colsample_btree" -> 0.63
			, "min_child_weight" -> 9
			, "gamma" -> 0
			, "eval_metric" -> "rmse"
			, "tree_method" -> "exact"
		).map(f => (f._1, f._2.asInstanceOf[AnyRef])), 200, 2)
			val prediction = UberXGBoostModel.labelPredict(testData, booster = model).cache
			prediction.count()
			assert(prediction.count == 288)
	}

	it should "Accept data and execute xgboost big model" in {
    ClusterSettings.kryoBufferMaxSize = Some("70m")
//    ClusterSettings.taskCpus = 4
    val train = Dataset(context, s"$defaultFilePath/data/RossmannTrain.csv")

    val trainData = train
      .select(
        "Store",
        "Sales",
        "DayOfWeek",
        "Date",
        "Open",
        "Promo",
        "StateHoliday",
        "SchoolHoliday")
      .cache
    val test = Dataset(context, s"$defaultFilePath/data/RossmannTest.csv")

    val testData = test

    val (prediction, model) = ForecastPredictor().predictBigModelFuture(
      trainData,
      testData,
      SupportedAlgorithm.XGBoostAlgorithm,
      "Sales",
      "Id",
      "Date",
      Seq("Store", "DayOfWeek", "Date", "Date2", "Date3"))
    assert(prediction.count == 288)
  }

	it should "execute xgboost big model to binary classification" in {
		ClusterSettings.kryoBufferMaxSize = Some("70m")
		val train = Dataset(context, s"$defaultFilePath/data/bank-train.csv")
		val test = Dataset(context, s"$defaultFilePath/data/bank-test.csv")

		val trainIdCol = IUberdataForecastUtil.createIdColColumn(train, context)
		val testIdCol = IUberdataForecastUtil.createIdColColumn(test, context).drop(test.col("y"))

		val (prediction, model) = BinaryClassification().predict(
			trainIdCol,
			testIdCol,
			SupportedAlgorithm.XGBoostAlgorithm,
			"y",
			"idCol",
			Seq("age", "job", "marital",
				"education", "housing", "loan", "default",
				"duration", "campaign", "pdays",
				"previous", "empvarrate",
				"conspriceidx", "consconfidx", "euribor3m", "nremployed"))
		assert(prediction.count == 250)

		val metrics = new BinaryClassificationMetrics(joinTestPredictionDfMetrics(prediction, testIdCol, "idCol", "idCol"))
		print(metrics.areaUnderPR )
		assert(metrics.areaUnderPR > 0.44 )
	}

	it should "execute xgboost big model to binary classification using windows approach" in {
		ClusterSettings.kryoBufferMaxSize = Some("70m")
		val train = Dataset(context, s"$defaultFilePath/data/bank-train.csv")
		val test = Dataset(context, s"$defaultFilePath/data/bank-test.csv")

		val trainIdCol = IUberdataForecastUtil.createIdColColumn(train, context)
		val testIdCol = IUberdataForecastUtil.createIdColColumn(test, context).drop(test.col("y"))

		val (conversionRate, prediction, areaUnderPR, areaUnderROC) = BinaryClassification().predictUsingWindowsApproach(
			trainIdCol,
			testIdCol,
			SupportedAlgorithm.XGBoostAlgorithm,
			"y",
			"idCol",
			Seq("age", "marital", "housing", "loan", "duration", "campaign",
				"pdays", "previous", "empvarrate", "conspriceidx", "consconfidx", "euribor3m", "nremployed"),
			2000,
			Map.empty[String, Any],
			200)

		assert(conversionRate.count == 10)
		assert(conversionRate.filter("decile=10").first.getDouble(1) > 0.10)
		assert(prediction.count == 250)
		assert(areaUnderPR > 0.60)
		assert(areaUnderROC > 0.80)

	}

	it should "throw an exception if trainingWindowSize is greater than train dataframe size while executing " +
		"xgboost big model to binary classification using windows approach" in {
		ClusterSettings.kryoBufferMaxSize = Some("70m")
		val train = Dataset(context, s"$defaultFilePath/data/bank-train.csv")
		val test = Dataset(context, s"$defaultFilePath/data/bank-test.csv")

		val trainIdCol = IUberdataForecastUtil.createIdColColumn(train, context)
		val testIdCol = IUberdataForecastUtil.createIdColColumn(test, context).drop(test.col("y"))

		intercept[IllegalArgumentException] {
			val (conversionRate, prediction, areaUnderPR, areaUnderROC) = BinaryClassification().predictUsingWindowsApproach(
				trainIdCol,
				testIdCol,
				SupportedAlgorithm.XGBoostAlgorithm,
				"y",
				"idCol",
				Seq("age", "marital", "housing", "loan", "duration", "campaign",
					"pdays", "previous", "empvarrate", "conspriceidx", "consconfidx", "euribor3m", "nremployed"),
				2000,
				Map.empty[String, Any],
				2000)
		}

	}

	it should "throw an exception if train dataframe size is smaller than test dataframe size while executing " +
		"xgboost big model to binary classification using windows approach" in {
		ClusterSettings.kryoBufferMaxSize = Some("70m")
		val train = Dataset(context, s"$defaultFilePath/data/bank-train.csv")
		val test = Dataset(context, s"$defaultFilePath/data/bank-test.csv")

		val trainIdCol = IUberdataForecastUtil.createIdColColumn(train, context)
		val testIdCol = IUberdataForecastUtil.createIdColColumn(test, context).drop(test.col("y"))

		intercept[IllegalArgumentException] {
			val (conversionRate, prediction, areaUnderPR, areaUnderROC) = BinaryClassification().predictUsingWindowsApproach(
				trainIdCol.where("idCol <= 100"),
				testIdCol,
				SupportedAlgorithm.XGBoostAlgorithm,
				"y",
				"idCol",
				Seq("age", "marital", "housing", "loan", "duration", "campaign",
					"pdays", "previous", "empvarrate", "conspriceidx", "consconfidx", "euribor3m", "nremployed"),
				2000,
				Map.empty[String, Any],
				200)
		}

	}

	def joinTestPredictionDfMetrics(prediction : DataFrame,
	                                test : DataFrame,
	                                idPredit : String,
	                                idTest : String): RDD[(Double, Double)] = {
		val joindf = prediction.join(test, test(idTest) === prediction(idPredit), "inner")
		joindf.select(joindf("prediction").cast(DoubleType).as("prediction"),
									joindf("y").cast(DoubleType).as("y")).rdd
			.map(x => (x.toSeq(0).asInstanceOf[Double], x.toSeq(1).asInstanceOf[Double]))
	}

  override def copy(extra: ParamMap): Params = ???

  override val uid: String = "aaa"
}
