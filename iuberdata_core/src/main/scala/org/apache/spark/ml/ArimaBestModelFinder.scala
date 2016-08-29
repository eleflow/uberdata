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
class ArimaBestModelFinder[G](
  override val uid: String
)(implicit kt: ClassTag[G])
    extends BestModelFinder[G, ArimaModel[G]]
    with ArimaParams
    with DefaultParamsWritable
    with HasNFutures
    with TimeSeriesBestModelFinder
    with Logging {
  def this()(implicit kt: ClassTag[G]) = this(Identifiable.randomUID("arima"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[G]): this.type =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type =
    set(estimatorParamMaps, value)

  def setNFutures(value: Int): this.type = set(nFutures, value)

  override def setValidationCol(value: String): this.type = set(validationCol, value)

  def setFeaturesCol(features: String): this.type = set(featuresCol, features)

  def setGroupByCol(groupBy: String): this.type = set(groupByCol, groupBy)

  def getOrdering(metricName: String): Ordering[Double] = {
    metricName match {
      case "re" => Ordering.Double.reverse
      case _ => Ordering.Double
    }
  }

  def modelEvaluation(
    idModels: RDD[(G, Row, Seq[(ParamMap, UberArimaModel)])]
  ): RDD[(G, (UberArimaModel, Seq[ModelParamEvaluation[G]]))] = {
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

  override protected def train(dataSet: DataFrame): ArimaModel[G] = {
    val splitDs = split(dataSet, $(nFutures))
    val labelModel = splitDs.rdd.map(train)
    new ArimaModel[G](uid, modelEvaluation(labelModel))
      .setValidationCol($(validationCol))
      .asInstanceOf[ArimaModel[G]]
  }

  def train(row: Row): (G, Row, Seq[(ParamMap, UberArimaModel)]) = {
    val groupId = row.getAs[G]($(groupByCol))
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
              s"in groupId $groupId"
          )
          None
      }
    }.toSeq
    (groupId, row, result)
  }
}
case class ModelParamEvaluation[G](id: G,
                                   metricResult: Double,
                                   params: ParamMap,
                                   metricName: Option[String] = None,
                                   algorithm: Algorithm)

object ArimaBestModelFinder
    extends DefaultParamsReadable[ArimaBestModelFinder[_]] {

  override def load(path: String): ArimaBestModelFinder[_] = super.load(path)
}

class ArimaTrainingSummary[G](predictions: DataFrame,
                              predictionCol: String,
                              groupByCol: String,
                              model: ArimaModel[G],
                              diagInvAtWA: Array[Double],
                              val featuresCol: String,
                              val objectiveHistory: Array[Double])
    extends ARIMALinearSummary[G](
      predictions,
      predictionCol,
      groupByCol,
      model,
      diagInvAtWA
    )
