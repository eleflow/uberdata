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

import com.cloudera.sparkts.models._
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.param.shared.{HasTimeSeriesEvaluator, HasWindowParams}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
  * Created by dirceu on 31/05/16.
  */
class ForecastBestModelFinder[I, M <: ForecastBaseModel[M]](
  override val uid: String
)(implicit kt: ClassTag[I])
    extends HoltWintersBestModelEvaluation[I, M]
    with HoltWintersParams
    with DefaultParamsWritable
    with HasWindowParams
    with ArimaParams
    with HasTimeSeriesEvaluator[I]
    with TimeSeriesBestModelFinder {
//    with Logging {

  def this()(implicit kt: ClassTag[I]) =
    this(Identifiable.randomUID("BestForecast"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[I]) =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type =
    set(estimatorParamMaps, value)

  def setNFutures(value: Int) = set(nFutures, value)

  override def setValidationCol(value: String) = set(validationCol, value)

  def setFeaturesCol(label: String) = set(featuresCol, label)

  def setLabelCol(label: String) = set(labelCol, label)

  def setWindowParams(params: Seq[Int]) = set(windowParams, params)

  def movingAverageEvaluation(
    row: Row,
    model: UberHoltWintersModel,
    broadcastEvaluator: Broadcast[TimeSeriesEvaluator[I]],
    id: I
  ) = {
    val features =
      row.getAs[org.apache.spark.ml.linalg.Vector]($(featuresCol))
    log.warn(
      s"Evaluating forecast for id $id, with parameters alpha ${model.alpha}, beta ${model.beta} and gamma ${model.gamma}"
    )
    val expectedResult =
      row.getAs[org.apache.spark.mllib.linalg.Vector](partialValidationCol)
    val forecastToBeValidated = Vectors.dense(new Array[Double]($(nFutures)))
    model.forecast(org.apache.spark.mllib.linalg.Vectors.fromML(features), forecastToBeValidated).toArray
    $(windowParams).map { windowSize =>
      val movingAverageToBeValidate =
        MovingAverageCalc.simpleMovingAverageArray(forecastToBeValidated.toArray, windowSize)
      val toBeValidated = expectedResult.toArray.zip(movingAverageToBeValidate)
      val metric = broadcastEvaluator.value.evaluate(toBeValidated)
      val metricName = broadcastEvaluator.value.getMetricName
      val params = ParamMap().put(new ParamPair[Int](windowParam, windowSize))
      (model,
       new ModelParamEvaluation[I](
         id,
         metric,
         params,
         Some(metricName),
         SupportedAlgorithm.MovingAverage8
       ))
    }
  }

  def modelEvaluation(
    idModels: RDD[(I, Row, Seq[(ParamMap, TimeSeriesModel)])]
  ) = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)

    val ordering = TimeSeriesEvaluator.ordering(eval.getMetricName)
    idModels.map {
      case (id, row, models) =>
        val evaluatedModels = models.flatMap {
          case (parameters, model: UberArimaModel) =>
            Seq(
              arimaEvaluation(row, model, broadcastEvaluator, id, parameters)
            )
          case (parameters, model: UberHoltWintersModel) =>
            Seq(holtWintersEvaluation(row, model, broadcastEvaluator, id)) ++
              movingAverageEvaluation(row, model, broadcastEvaluator, id)
        }
        val sorted = evaluatedModels.sortBy(_._2.metricResult)(ordering)
        log.warn(s"best model reach ${sorted.head._2.metricResult}")
        log.warn(s"best model was ${sorted.head._2.algorithm}")
        log.warn(s"best model params ${sorted.head._2.params}")

        val (bestModel, _) = sorted.head
        (id, (bestModel, sorted))
    }
  }

  override protected def train(dataSet: Dataset[_]): M = {
    val splitDs = split(dataSet, $(nFutures))
    val idModels = splitDs.rdd.map(train)
    new ForecastBestModel[I](uid, modelEvaluation(idModels))
      .setValidationCol($(validationCol))
      .asInstanceOf[M]
  }

  def train(row: Row): (I, Row, Seq[(ParamMap, TimeSeriesModel)]) = {
    val id = row.getAs[I]($(labelCol))

    val holtWintersResults = try {
      val holtWinters =
        UberHoltWintersModel.fitModelWithBOBYQA(org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs($(featuresCol))), $(nFutures))
      val params = ParamMap()
        .put(ParamPair(alpha, holtWinters.alpha))
        .put(ParamPair(beta, holtWinters.beta))
        .put(ParamPair(gamma, holtWinters.gamma))
      Some((params, holtWinters))
    } catch {
      case e: Exception =>
        log.error(
          s"Got the following Exception ${e.getLocalizedMessage} in id $id"
        )
        None
    }
    val arimaResults =
      $(estimatorParamMaps).map { params =>
        val q = params.getOrElse(arimaQ, 0)
        val p = params.getOrElse(arimaP, 0)
        val d = params.getOrElse(arimaD, 0)
        try {
          val result =
            UberArimaModel.fitModel(p, d, q, org.apache.spark.mllib.linalg.Vectors.fromML(row.getAs($(featuresCol))))
          val params = ParamMap(
            ParamPair(arimaP, result.p),
            ParamPair(arimaQ, result.q),
            ParamPair(arimaD, result.d)
          )
          Some(params, result)
        } catch {
          case e: Exception =>
            log.error(
              s"Got the following Exception ${e.getLocalizedMessage} in id $id"
            )
            None
        }
      }
    (id, row, (Seq(holtWintersResults) ++ arimaResults).flatten)
  }
}
