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

import com.cloudera.sparkts.models.UberArimaModel
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 31/05/16.
  */
abstract class BestModelFinder[L, M <: ForecastBaseModel[M]](implicit kt: ClassTag[L],
                                                             ord: Ordering[L] = null)
    extends Estimator[M]
    with PredictorParams
    with HasTimeSeriesEvaluator[L]
    with HasEstimatorParams
    with HasNFutures
    with HasValidationCol {

  lazy val partialValidationCol = s"partial${$(validationCol)}"
  lazy val inputOutputDataType = new VectorUDT

  def setValidationCol(colName: String) = set(validationCol, colName)

  protected def train(dataSet: Dataset[_]): M

  override def fit(dataSet: Dataset[_]): M = {
    copyValues(train(dataSet).setParent(this))
  }

  protected def split(dataSet: Dataset[_], nFutures: Int) = {
    dataSet.rdd.map { case (row: Row ) =>
      val features = row.getAs[org.apache.spark.ml.linalg.DenseVector]($(featuresCol))
      if (features.size - nFutures <= 0) {
        throw new IllegalArgumentException(
          s"Row ${row.toSeq.mkString(",")} " +
            s"has less timeseries attributes than nFutures")
      }
    }
    val data = dataSet.rdd.map { case (row: Row ) =>
      val featuresIndex = row.fieldIndex($(featuresCol))
      val features = row.getAs[org.apache.spark.ml.linalg.DenseVector](featuresIndex)
      val trainSize = features.size - nFutures
      val (validationFeatures, toBeValidated) = features.toArray.splitAt(trainSize)
      val validationRow = row.toSeq.updated(featuresIndex, Vectors.dense(validationFeatures)) :+
          Vectors.dense(toBeValidated)
      Row(validationRow: _*)
    }
    val context = dataSet.sqlContext
    context
      .createDataFrame(
        data,
        dataSet.schema.add(StructField(partialValidationCol, new VectorUDT))
      )
      .cache
  }

  def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): Estimator[M] = {
    val that = this.getClass
      .getConstructor(
        classOf[String],
        classOf[scala.reflect.ClassTag[L]],
        classOf[scala.math.Ordering[L]]
      )
      .newInstance(uid, kt, ord)
      .setValidationCol($(validationCol))
    copyValues(that, extra)
  }

  protected def arimaEvaluation(
    row: Row,
    model: UberArimaModel,
    broadcastEvaluator: Broadcast[TimeSeriesEvaluator[L]],
    id: L,
    parameters: ParamMap): (UberArimaModel, ModelParamEvaluation[L]) = {
    val features = row.getAs[org.apache.spark.ml.linalg.Vector]($(featuresCol))
    log.warn(
      s"Evaluating forecast for id $id, with parameters p ${model.p}, d ${model.d} " +
        s"and q ${model.q}")

    val featuresConverted: org.apache.spark.mllib.linalg.Vector  =  org.apache.spark.mllib.linalg.Vectors.fromML(features);

    val (forecastToBeValidated, _) =
      model.forecast(featuresConverted, $(nFutures)).toArray.splitAt(features.size)
    val toBeValidated = features.toArray.zip(forecastToBeValidated)
    val metric = broadcastEvaluator.value.evaluate(toBeValidated)
    val metricName = broadcastEvaluator.value.getMetricName
    (model,
     new ModelParamEvaluation[L](
       id,
       metric,
       parameters,
       Some(metricName),
       SupportedAlgorithm.Arima))
  }
}
