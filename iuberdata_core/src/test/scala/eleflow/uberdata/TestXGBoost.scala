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
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}
import org.scalatest.{FlatSpec, Matchers, Suite}

/**
  * Created by dirceu on 25/08/16.
  */
class TestXGBoost
    extends FlatSpec
    with Matchers
    with BeforeAndAfterWithContext
    with HasXGBoostParams { this: Suite =>

  "XGBostAlgorithm" should "execute xgboost " in {
//		val train = Dataset(context, s"$defaultFilePath/data/RossmannTrain.csv")
//
//		val trainData = train.formatDateValues("Date", DayMonthYear).select("Store", "Sales",
//			"DayOfWeek", "Date1", "Date2", "Date3", "Open", "Promo", "StateHoliday", "SchoolHoliday")
//			.cache
//		val test = Dataset(context, s"$defaultFilePath/data/RossmannTest.csv")
//
//		val testData = test.formatDateValues("Date", DayMonthYear).map{
//		row =>
//			XGBLabeledPoint.fromDenseVector(row.getAs[Long]("Id"), Array(row.getAs[Long]("Store").toFloat,
//				row.getAs[Long]("DayOfWeek").toFloat, row.getAs[Int]("Date1").toFloat,
//				row.getAs[Int]("Date2").toFloat,row.getAs[Int]("Date3").toFloat))
//		}
//
//		val trainLabel = trainData.map{
//			row =>
//				LabeledPoint(row.getAs[Long]("Sales").toDouble,
//					Vectors.dense(Array(row.getAs[Long]("Store").toDouble,
//						row.getAs[Long]("DayOfWeek").toDouble, row.getAs[Int]("Date1").toDouble,
//						row.getAs[Int]("Date2").toDouble, row.getAs[Int]("Date3").toDouble)))
//		}
//
//		val model = UberXGBoostModel.train(trainLabel, Map[String, Any](
//			"silent" -> 1
//			, "objective" -> "reg:linear"
//			, "booster" -> "gbtree"
//			, "eta" -> 0.0225
//			, "max_depth" -> 26
//			, "subsample" -> 0.63
//			, "colsample_btree" -> 0.63
//			, "min_child_weight" -> 9
//			, "gamma" -> 0
//			, "eval_metric" -> "rmse"
//			, "tree_method" -> "exact"
//		).map(f => (f._1, f._2.asInstanceOf[AnyRef])), 200, 2)
//			val prediction = UberXGBoostModel.labelPredict(testData, booster = model).cache
//			prediction.count()
//			assert(prediction.count == 288)
//	}
//
//	it should "Accept data and execute xgboost big model" in {
    ClusterSettings.kryoBufferMaxSize = Some("70m")
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
      Seq("Store", "DayOfWeek", "Date"))
    assert(prediction.count == 288)
  }

  override def copy(extra: ParamMap): Params = ???

  override val uid: String = "aaa"
}
