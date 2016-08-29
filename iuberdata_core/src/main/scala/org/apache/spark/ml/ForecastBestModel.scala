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

package org.apache.spark.ml

import com.cloudera.sparkts.models.{
  UberHoltWintersModel,
  TimeSeriesModel,
  UberArimaModel
}
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 01/06/16.
  */
class ForecastBestModel[L](
  override val uid: String,
  val models: RDD[
    (L, (TimeSeriesModel, Seq[(TimeSeriesModel, ModelParamEvaluation[L])]))
  ]
)(implicit kt: ClassTag[L])
    extends ForecastBaseModel[ForecastBestModel[L]]
    with HasLabelCol
    with HasFeaturesCol
    with ForecastPipelineStage {

  override def copy(extra: ParamMap): ForecastBestModel[L] = {
    val newModel = copyValues(new ForecastBestModel[L](uid, models), extra)
    newModel.setParent(parent)
  }

  override def transformSchema(schema: StructType): StructType = {
    super
      .transformSchema(schema)
      .add(StructField("featuresValidation", new VectorUDT))
  }

  def evaluateParams(models: Seq[(TimeSeriesModel, ModelParamEvaluation[L])],
                     features: Vector,
                     nFut: Broadcast[Int]): Seq[Object] = {
    val (bestModel, modelParamEvaluation) = models.head
    try {
      modelParamEvaluation.algorithm match {
        case SupportedAlgorithm.Arima =>
          val a = bestModel.asInstanceOf[UberArimaModel]
          val (featuresPrediction, forecastPrediction) =
            a.forecast(features, nFut.value).toArray.splitAt(features.size)
          Seq(
            Vectors.dense(forecastPrediction),
            SupportedAlgorithm.Arima.toString,
            a.params,
            Vectors.dense(featuresPrediction)
          )
        case SupportedAlgorithm.HoltWinters =>
          val h = bestModel.asInstanceOf[UberHoltWintersModel]
          val forecast = Vectors.dense(new Array[Double](nFut.value))
          h.forecast(features, forecast)
          Seq(
            forecast,
            SupportedAlgorithm.HoltWinters.toString,
            h.params,
            features
          )
        case SupportedAlgorithm.MovingAverage8 =>
          val windowSize = modelParamEvaluation.params.toSeq
            .map(f => (f.param.name, f.value.asInstanceOf[Int]))
            .toMap
          val h = bestModel.asInstanceOf[UberHoltWintersModel]
          val forecast =
            Vectors.dense(new Array[Double](windowSize.values.head))
          h.forecast(features, forecast)
          val movingAverageForecast = Vectors.dense(
            MovingAverageCalc.simpleMovingAverageArray(
              forecast.toArray,
              windowSize.values.head
            )
          )
          Seq(
            movingAverageForecast,
            SupportedAlgorithm.MovingAverage8.toString,
            windowSize.map(f => (f._1, f._2.toString)),
            features
          )
      }
    } catch {
      case e: Exception =>
        log.error("Error when predicting ")
        e.printStackTrace()
        evaluateParams(models.tail, features, nFut)
    }
  }

  override def transform(dataSet: DataFrame): DataFrame = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema)

    val scContext = dataSet.sqlContext.sparkContext
    //TODO fazer com que os modelos invalidos voltem numeros absurdos

    val joined = models.join(dataSet.map(r => (r.getAs[L]($(labelCol)), r)))

    val featuresColName =
      dataSet.sqlContext.sparkContext.broadcast($(featuresCol))
    val nFut = scContext.broadcast($(nFutures))
    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row
          .getAs[org.apache.spark.mllib.linalg.Vector](featuresColName.value)
        val prediction = {
          evaluateParams(metrics, features, nFut)
        }
        Row(row.toSeq ++ prediction: _*)
    }
    dataSet.sqlContext.createDataFrame(predictions, predSchema)
  }
}
