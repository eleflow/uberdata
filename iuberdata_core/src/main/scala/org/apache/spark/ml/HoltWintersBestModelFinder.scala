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
//import org.apache.spark.Logging
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasGroupByCol
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
  * Created by dirceu on 19/05/16.
  */
class HoltWintersBestModelFinder[G](
  override val uid: String
)(implicit kt: ClassTag[G])
    extends HoltWintersBestModelEvaluation[G, HoltWintersModel[G]]
    with DefaultParamsWritable
    with HasGroupByCol
    with TimeSeriesBestModelFinder {
//    with Logging {

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[G]): this.type =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type =
    set(estimatorParamMaps, value)

  def setNFutures(value: Int): this.type = set(nFutures, value)

  override def setValidationCol(value: String): this.type = set(validationCol, value)

  def setLabelCol(label: String): this.type = set(labelCol, label)

  def setGroupByCol(groupBy: String): this.type = set(groupByCol, Some(groupBy))

  def this()(implicit kt: ClassTag[G]) = this(Identifiable.randomUID("arima"))

  def modelEvaluation(
    idModels: RDD[(G, Row, Option[UberHoltWintersModel])]
  ): RDD[(G, (UberHoltWintersModel, ModelParamEvaluation[G]))] = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)
    idModels.filter(_._3.isDefined).map {
      case (id, row, models) =>
        val evaluatedModels = models.map { model =>
          holtWintersEvaluation(row, model, broadcastEvaluator, id)
        }.head
        log.warn(s"best model reach ${evaluatedModels._2.metricResult}")
        (id, evaluatedModels)
    }
  }

  override protected def train(dataSet: Dataset[_]): HoltWintersModel[G] = {
    val splitDs = split(dataSet, $(nFutures))
    val idModels = splitDs.rdd.map(train)
    new HoltWintersModel[G](uid, modelEvaluation(idModels))
      .setValidationCol($(validationCol))
      .asInstanceOf[HoltWintersModel[G]]
  }

  def train(row: Row): (G, Row, Option[UberHoltWintersModel]) = {
    val id = row.getAs[G]($(groupByCol).get)

    val result = try {
      val dense = row.getAs[org.apache.spark.ml.linalg.DenseVector]($(featuresCol))
      val ts:org.apache.spark.mllib.linalg.Vector  = org.apache.spark.mllib.linalg.Vectors.dense(dense.toArray);
      Some(
        UberHoltWintersModel.fitModelWithBOBYQA(ts, $(nFutures))
      )
    } catch {
      case e: Exception =>
        log.error(
          s"Got the following Exception ${e.getLocalizedMessage} in id $id"
        )
        None
    }
    (id, row, result)
  }
}

object HoltWintersBestModelFinder extends DefaultParamsReadable[HoltWintersBestModelFinder[_]] {

  override def load(path: String): HoltWintersBestModelFinder[_] =
    super.load(path)
}
