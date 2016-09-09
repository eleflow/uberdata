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

import com.cloudera.sparkts.models.UberXGBoostModel
import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.core.data.DataTransformer
import eleflow.uberdata.core.util.ClusterSettings
import eleflow.uberdata.models.UberXGBOOSTModel
import org.apache.spark.Logging
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasIdCol, HasTimeCol, HasXGBoostParams}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 24/08/16.
  */
class XGBoostBestBigModelFinder[L, G](override val uid: String)(implicit gt: ClassTag[G],
                                                                lt: ClassTag[L])
    extends BaseXGBoostBestModelFinder[G, XGBoostBigModel[G]]
    with DefaultParamsWritable
    with HasXGBoostParams
    with HasIdCol
		with HasTimeCol
    with TimeSeriesBestModelFinder
    with Logging {
  def this()(implicit gt: ClassTag[G], lt: ClassTag[L]) =
    this(Identifiable.randomUID("xgboostbig"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[G]): XGBoostBestBigModelFinder[L, G] =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  override def setValidationCol(value: String): XGBoostBestBigModelFinder[L, G] =
    set(validationCol, value)

  def setLabelCol(label: String): this.type = set(labelCol, label)

  def setIdCol(id: String): this.type = set(idCol, id)

	def setTimeCol(time: String): this.type = set(timeCol, time)

  def setXGBoostParams(params: Map[String, Any]): this.type = set(xGBoostParams, params)

  def getOrdering(metricName: String): Ordering[Double] = {
    metricName match {
      case "re" => Ordering.Double.reverse
      case _ => Ordering.Double
    }
  }

  def modelEvaluation(idModels: RDD[(G, Row, Seq[(ParamMap, UberXGBOOSTModel)])])
    : RDD[(G, (UberXGBOOSTModel, Seq[ModelParamEvaluation[G]]))] = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)
    val ordering = TimeSeriesEvaluator.ordering(eval.getMetricName)
    idModels.map {
      case (id, row, models) =>
        val evaluatedModels = models.map {
          case (parameters, model) =>
            (model,
             xGBoostEvaluation(row, model.boosterInstance, broadcastEvaluator, id, parameters))
        }
        val sorted = evaluatedModels.sortBy(_._2.metricResult)(ordering)
        log.warn(s"best model reach ${sorted.head._2.metricResult}")
        log.warn(s"best model params ${sorted.head._2.params}")

        val (bestModel, _) = sorted.head
        (id, (bestModel, sorted.map(_._2)))
    }
  }

  override protected def train(dataSet: DataFrame): XGBoostBigModel[G] = {
    val labeledPointDataSet = dataSet.map { row =>
      val values = row
        .getAs[org.apache.spark.mllib.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
        .toArray
      val label = DataTransformer.toFloat(row.getAs[L]($(labelCol)))
      LabeledPoint(label, Vectors.dense(values))
    }
    val booster = UberXGBoostModel.train(
      labeledPointDataSet,
      $(xGBoostParams),
      $(xGBoostRounds),
      ClusterSettings.defaultParallelism.getOrElse(
        dataSet.sqlContext.sparkContext.getConf
          .getInt("spark.default.parallelism", ClusterSettings.xgBoostWorkers)))

    new XGBoostBigModel[G](uid, Seq((new ParamMap(), booster)))
      .setIdcol($(idCol))
			.setLabelcol($(labelCol))
      .setTimecol($(timeCol))
  }
}

object XGBoostBestBigModelFinder extends DefaultParamsReadable[XGBoostBestSmallModelFinder[_, _]] {
  override def load(path: String): XGBoostBestSmallModelFinder[_, _] = super.load(path)
}
