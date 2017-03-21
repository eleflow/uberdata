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

package org.apache.spark.ml.evaluation

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{
  HasFeaturesCol,
  HasLabelCol,
  HasPredictionCol,
  HasValidationCol
}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, _}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.reflect.ClassTag

/**
  * Created by dirceu on 26/04/16.
  */
final class TimeSeriesEvaluator[L](
  override val uid: String
)(implicit kt: ClassTag[L])
    extends KeyValueEvaluator[L]
    with HasPredictionCol
    with HasLabelCol
    with HasValidationCol
    with DefaultParamsWritable {

  def this()(implicit kt: ClassTag[L]) =
    this(Identifiable.randomUID("regEval"))

  /**
    * param for metric name in evaluation (supports `"rmse"` (default), `"mse"`, `"r2"`, and `"mae"`)
    *
    * Because we will maximize evaluation value (ref: `CrossValidator`),
    * when we evaluate a metric that is needed to minimize (e.g., `"rmse"`, `"mse"`, `"mae"`),
    * we take and output the negative of this metric.
    *
    * @group param
    */
  val metricName: Param[String] = {
    val allowedParams =
      ParamValidators.inArray(Array("mse", "rmse", "r2", "rmspe", "mae"))
    new Param(
      this,
      "metricName",
      "metric name in evaluation (mse|rmse|r2|mae)",
      allowedParams
    )
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  def setValidationCol(value: String): this.type = set(validationCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "rmse")

  override def evaluate(dataSet: (L, (Int, Vector))): RDD[(L, (Int, Double))] =
    throw new UnsupportedOperationException

  def evaluate(dataSet: Array[(Double, Double)]): Double = {
    val metrics = new TimeSeriesSmallModelRegressionMetrics(dataSet)
    $(metricName) match {
      case "rmse" => metrics.rootMeanSquaredError
      case "mse" => metrics.meanSquaredError
      case "rmspe" => metrics.rootMeanSquaredPercentageError
      case "r2" => metrics.r2
      case "mae" => metrics.meanAbsoluteError
    }
  }

  def evaluate(dataSet: DataFrame): RDD[(L, (Int, Double))] = {
    val schema = dataSet.schema
    val validationColName = $(validationCol)
    val validationColType = schema($(validationCol)).dataType

    val labelColName = $(labelCol)
    val labelType = schema($(labelCol)).dataType

    val predictionAndLabels = (validationColType, labelType) match {
      case (p: VectorUDT, f: VectorUDT) =>
        dataSet.rdd.map { f =>
          val label = f.getAs[L](0)
          val prediction = f.getAs[org.apache.spark.mllib.linalg.Vector](1)
          val feature = f.getAs[org.apache.spark.mllib.linalg.Vector](2)
          val modelIndex = f.getAs[Int](3)
          (label, modelIndex, feature.toArray.zip(prediction.toArray))
        }
      case _ =>
        dataSet
          .select(
            col(validationColName).cast(DoubleType),
            col(labelColName).cast(DoubleType)
          )
          .rdd.map { row =>
            val label = row.getAs[L](0)
            val prediction = row.getAs[Double](1)
            val feature = row.getAs[Double](2)
            val modelIndex = row.getAs[Int](3)
            (label, modelIndex, Array((prediction, feature)))
          }
    }

    val metrics =
      new TimeSeriesRegressionMetrics[L](predictionAndLabels, isLargerBetter)
    val metric = $(metricName) match {
      case "rmse" => metrics.rootMeanSquaredError
      case "mse" => metrics.meanSquaredError
      case "r2" => metrics.r2
      case "mae" => metrics.meanAbsoluteError
    }
    metric
  }

  @Since("1.4.0")
  override def isLargerBetter: Boolean = $(metricName) == "r2"

  override def copy(extra: ParamMap): TimeSeriesEvaluator[L] =
    defaultCopy(extra)
}

object TimeSeriesEvaluator extends DefaultParamsReadable[TimeSeriesEvaluator[_]] {

  override def load(path: String): TimeSeriesEvaluator[_] = super.load(path)

  def ordering(metricName: String): Ordering[Double] =
    metricName match {
      case "r2" => Ordering.Double.reverse
      case "rmspe" | "rmse" | "mse" | "mae" => Ordering.Double
    }
}
