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
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasIdCol, HasTimeCol, HasXGBoostParams}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

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
    with TimeSeriesBestModelFinder {
  def this()(implicit gt: ClassTag[G], lt: ClassTag[L]) =
    this(Identifiable.randomUID("xgboostbig"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[G]): XGBoostBestBigModelFinder[L, G] =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  override def setValidationCol(value: String): XGBoostBestBigModelFinder[L, G] =
    set(validationCol, value)

  def setLabelCol(label: String): this.type = set(labelCol, label)

  def setIdCol(id: String): this.type = set(idCol, id)

  def setXGBoostLinearParams(params: Map[String, Any]): this.type =
    if (params.nonEmpty) {
      set(xGBoostRegLinearParams, params)
    } else this

  def setXGBoostBinaryParams(params: Map[String, Any]): this.type =
    if (params.nonEmpty) {
      set(xGBoostBinaryParams, params)
    } else this

  def setXGBoostRounds(rounds: Int): this.type = set(xGBoostRounds, rounds)

	def setTimeCol(time: String): this.type = set(timeCol, Some(time))

  def getOrdering(metricName: String): Ordering[Double] = {
    metricName match {
      case "re" => Ordering.Double.reverse
      case _ => Ordering.Double
    }
  }

  override protected def train(dataSet: Dataset[_]): XGBoostBigModel[G] = {
    val labeledPointDataSet = dataSet.rdd.map { case (row: Row) =>
      val values = row
        .getAs[org.apache.spark.ml.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
        .toArray
      val label = DataTransformer.toFloat(row.getAs[L]($(labelCol)))
      LabeledPoint(label, Vectors.dense(values))
    }.cache

    $(timeCol).isDefined match {
      case true =>
        val booster: XGBoostModel = getBooster(labeledPointDataSet,
          $(xGBoostRegLinearParams), $(xGBoostRounds))

        new XGBoostBigModelTimeSeries[G](uid, Seq((new ParamMap(), booster)))
        .setIdcol($(idCol))
        .setLabelcol($(labelCol))
        .setTimecol($(timeCol).get)

      case _ =>
        val booster: XGBoostModel = getBooster(labeledPointDataSet,
          $(xGBoostBinaryParams), $(xGBoostRounds))
        new XGBoostBigModel[G](uid, Seq((new ParamMap(), booster)))
        .setIdcol($(idCol))
        .setLabelcol($(labelCol))
    }
  }

  def getBooster(labeledPointDataSet: RDD[LabeledPoint],
                 xgboostParams : Map[String, Any],
                 rounds : Int ): XGBoostModel = {
    val booster = UberXGBoostModel.train(
      labeledPointDataSet,
      xgboostParams,
      rounds,
      ClusterSettings.xgBoostWorkers)
    booster
  }
}

object XGBoostBestBigModelFinder extends DefaultParamsReadable[XGBoostBestSmallModelFinder[_, _]] {
  override def load(path: String): XGBoostBestSmallModelFinder[_, _] = super.load(path)
}
