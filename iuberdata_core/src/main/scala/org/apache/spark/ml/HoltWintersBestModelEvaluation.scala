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

import com.cloudera.sparkts.models.UberHoltWintersModel
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

/**
  * Created by dirceu on 02/06/16.
  */
abstract class HoltWintersBestModelEvaluation[L, M <: ForecastBaseModel[M]](
  implicit kt: ClassTag[L],
  ord: Ordering[L] = null
) extends BestModelFinder[L, M]
    with HoltWintersParams {

  protected def holtWintersEvaluation(
    row: Row,
    model: UberHoltWintersModel,
    broadcastEvaluator: Broadcast[TimeSeriesEvaluator[L]],
    id: L
  ): (UberHoltWintersModel, ModelParamEvaluation[L]) = {
    val features =
      row.getAs[org.apache.spark.ml.linalg.Vector]($(featuresCol))
    log.warn(
      s"Evaluating forecast for id $id, with parameters " +
        s"alpha ${model.alpha}, beta ${model.beta} and gamma ${model.gamma}"
    )
    val expectedResult =
      row.getAs[org.apache.spark.ml.linalg.Vector](partialValidationCol)
    val forecastToBeValidated = Vectors.dense(new Array[Double]($(nFutures)))
    model.forecast(features, forecastToBeValidated).toArray
    val toBeValidated =
      expectedResult.toArray.zip(forecastToBeValidated.toArray)
    val metric = broadcastEvaluator.value.evaluate(toBeValidated)
    val metricName = broadcastEvaluator.value.getMetricName
    val params = ParamMap().put(
      ParamPair(gamma, model.gamma),
      ParamPair(beta, model.beta),
      ParamPair(alpha, model.alpha)
    )
    (model,
     new ModelParamEvaluation[L](
       id,
       metric,
       params,
       Some(metricName),
       SupportedAlgorithm.HoltWinters
     ))
  }
}
