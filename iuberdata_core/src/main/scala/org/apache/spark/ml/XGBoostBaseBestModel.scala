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

package org.apache.spark.ml

import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.core.data.DataTransformer
import eleflow.uberdata.enums.SupportedAlgorithm
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import ml.dmlc.xgboost4j.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasGroupByCol
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, FloatType, StructField, StructType}

/**
  * Created by dirceu on 30/08/16.
  */
trait BaseXGBoostBestModelFinder[G, M <: org.apache.spark.ml.ForecastBaseModel[M]]
    extends BestModelFinder[G, M]
    with HasGroupByCol {

  protected def buildTrainSchema(sparkContext: SparkContext): Broadcast[StructType] = sparkContext.broadcast {
    StructType(
      Seq(
        StructField($(groupByCol).get, FloatType),
        StructField(IUberdataForecastUtil.FEATURES_COL_NAME, ArrayType(new VectorUDT))))
  }


  protected def xGBoostEvaluation(row: Row,
                                  model: Booster,
                                  broadcastEvaluator: Broadcast[TimeSeriesEvaluator[G]],
                                  id: G,
                                  parameters: ParamMap): ModelParamEvaluation[G] = {
    val featuresArray = row
      .getAs[Array[org.apache.spark.ml.linalg.Vector]](IUberdataForecastUtil.FEATURES_COL_NAME)
      .map { vec =>
        val values = vec.toArray.map(DataTransformer.toFloat)
        LabeledPoint(values.head, null, values.tail)
      }
    val features = new DMatrix(featuresArray.toIterator)
    log.warn(s"Evaluating forecast for id $id, with xgboost")
    val prediction = model.predict(features).flatten
    val (forecastToBeValidated, _) = prediction.splitAt(featuresArray.length)
    val toBeValidated = featuresArray.zip(forecastToBeValidated)
    val metric = broadcastEvaluator.value.evaluate(toBeValidated.map(f =>
      (f._1.label.toDouble, f._2.toDouble)))
    val metricName = broadcastEvaluator.value.getMetricName
    new ModelParamEvaluation[G](
      id,
      metric,
      parameters,
      Some(metricName),
      SupportedAlgorithm.XGBoostAlgorithm)
  }
}
