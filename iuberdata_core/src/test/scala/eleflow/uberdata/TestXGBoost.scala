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

import eleflow.uberdata.core.data.Dataset
import eleflow.uberdata.core.data.Dataset._
import eleflow.uberdata.core.enums.DateSplitType._
import eleflow.uberdata.models.UberXGBOOSTModel
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.HasXGBoostParams
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}
import org.scalatest.{FlatSpec, Matchers, Suite}

/**
 * Created by dirceu on 25/08/16.
 */
class TestXGBoost extends FlatSpec with Matchers with BeforeAndAfterWithContext
	with HasXGBoostParams {
	this: Suite =>

		"XGBostAlgorithm" should "execute xgboost bigmodel" in {
		val train = Dataset(context, s"$defaultFilePath/data/RossmannTrain.csv")
		train.applyColumnTypes(Seq(DoubleType, DoubleType, TimestampType, DoubleType, DoubleType,
			DoubleType, DoubleType, StringType, StringType))
		val trainData = train.formatDateValues("Date", DayMonthYear).select("Store", "Sales",
			"DayOfWeek", "Date1", "Date2", "Date3", "Open", "Promo", "StateHoliday", "SchoolHoliday")
			.cache
		val test = Dataset(context, s"$defaultFilePath/data/RossmannTest.csv")
		test.applyColumnTypes(Seq(DoubleType, DoubleType, DoubleType, TimestampType, DoubleType,
			DoubleType, StringType, StringType))
		val testData = test.formatDateValues("Date", DayMonthYear).map{
		row =>
			Vectors.dense(Array(row.getAs[Double]("Store"), row.getAs[Double]("DayOfWeek"),
				row.getAs[Double]("Date1"), row.getAs[Double]("Date2"), row.getAs[Double]("Date3")))
		}

		val trainLabel = trainData.map{
			row =>
				LabeledPoint(row.getAs[Double]("Sales"),
					Vectors.dense(Array(row.getAs[Double]("Store"), row.getAs[Double]("DayOfWeek"),
						row.getAs[Double]("Date1"), row.getAs[Double]("Date2"), row.getAs[Double]("Date3"))))
		}

		val model = UberXGBOOSTModel.fitSparkModel(trainLabel,Map[String, Any](
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
		).map(f => (f._1, f._2.asInstanceOf[AnyRef])),200)
			val prediction = model.predict(testData).cache
			prediction.count()
			assert(prediction.count == 36)
	}

	override def copy(extra: ParamMap): Params = ???

	override val uid: String = "aaa"
}
