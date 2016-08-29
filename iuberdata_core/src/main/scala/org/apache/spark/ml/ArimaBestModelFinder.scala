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

import org.apache.spark.Logging
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression._
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.sql.{DataFrame, Row}
import com.cloudera.sparkts.models.UberArimaModel
import eleflow.uberdata.enums.SupportedAlgorithm.Algorithm
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.shared._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by dirceu on 12/04/16.
  * Treina e executa a validacao do melhor modelo, retornando o melhor modelo mas os parametros de avaliação de
  * todos os modelos.
  */
class ArimaBestModelFinder[L](
  override val uid: String
)(implicit kt: ClassTag[L])
    extends BestModelFinder[L, ArimaModel[L]]
    with ArimaParams
    with DefaultParamsWritable
    with HasNFutures
    with TimeSeriesBestModelFinder
    with Logging {
  def this()(implicit kt: ClassTag[L]) = this(Identifiable.randomUID("arima"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[L]) =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type =
    set(estimatorParamMaps, value)

  def setNFutures(value: Int) = set(nFutures, value)

  override def setValidationCol(value: String) = set(validationCol, value)

  def setFeaturesCol(label: String) = set(featuresCol, label)

  def setLabelCol(label: String) = set(labelCol, label)

  def getOrdering(metricName: String): Ordering[Double] = {
    metricName match {
      case "re" => Ordering.Double.reverse
      case _ => Ordering.Double
    }
  }

  def modelEvaluation(
    idModels: RDD[(L, Row, Seq[(ParamMap, UberArimaModel)])]
  ): RDD[(L, (UberArimaModel, Seq[ModelParamEvaluation[L]]))] = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)
    val ordering = TimeSeriesEvaluator.ordering(eval.getMetricName)
    idModels.map {
      case (id, row, models) =>
        val evaluatedModels = models.map {
          case (parameters, model) =>
            arimaEvaluation(row, model, broadcastEvaluator, id, parameters)
        }
        val sorted = evaluatedModels.sortBy(_._2.metricResult)(ordering)
        log.warn(s"best model reach ${sorted.head._2.metricResult}")
        log.warn(s"best model params ${sorted.head._2.params}")

        val (bestModel, _) = sorted.head
        (id, (bestModel.asInstanceOf[UberArimaModel], sorted.map(_._2)))
    }
  }

  override protected def train(dataSet: DataFrame): ArimaModel[L] = {
    val splitDs = split(dataSet, $(nFutures))
    val labelModel = splitDs.rdd.map(train)
    new ArimaModel[L](uid, modelEvaluation(labelModel))
      .setValidationCol($(validationCol))
      .asInstanceOf[ArimaModel[L]]
  }

  def train(row: Row): (L, Row, Seq[(ParamMap, UberArimaModel)]) = {
    val label = row.getAs[L]($(labelCol))
    val result = $(estimatorParamMaps).flatMap { params =>
      val q = params.getOrElse(arimaQ, 0)
      val p = params.getOrElse(arimaP, 0)
      val d = params.getOrElse(arimaD, 0)
      try {
        Some(
          (params, UberArimaModel.fitModel(p, d, q, row.getAs($(featuresCol))))
        )
      } catch {
        case e: Exception =>
          log.error(
            s"Got the following Exception ${e.getLocalizedMessage} when using params P $p, Q$q and D$d " +
              s"in label $label"
          )
          None
      }
    }.toSeq
    (label, row, result)
  }
}
case class ModelParamEvaluation[L](id: L,
                                   metricResult: Double,
                                   params: ParamMap,
                                   metricName: Option[String] = None,
                                   algorithm: Algorithm)

object ArimaBestModelFinder
    extends DefaultParamsReadable[ArimaBestModelFinder[_]] {

  override def load(path: String): ArimaBestModelFinder[_] = super.load(path)
}

class ArimaTrainingSummary[L](predictions: DataFrame,
                              predictionCol: String,
                              labelCol: String,
                              model: ArimaModel[L],
                              diagInvAtWA: Array[Double],
                              val featuresCol: String,
                              val objectiveHistory: Array[Double])
    extends ARIMALinearSummary[L](
      predictions,
      predictionCol,
      labelCol,
      model,
      diagInvAtWA
    )
