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

import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.core.data.DataTransformer
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import org.apache.spark.{SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasGroupByCol, HasIdCol, HasTimeCol, HasXGBoostParams}
import org.apache.spark.ml.regression.XGBoostLinearSummary
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
	* Created by dirceu on 30/06/16.
	*/
class XGBoostBestSmallModelFinder[L, G](override val uid: String)(implicit gt: ClassTag[G])
    extends BaseXGBoostBestModelFinder[G, XGBoostSmallModel[G]]
    with DefaultParamsWritable
    with HasXGBoostParams
    with TimeSeriesBestModelFinder
    with HasIdCol
    with HasTimeCol {
  def this()(implicit gt: ClassTag[G]) =
    this(Identifiable.randomUID("xgboostsmall"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[G]): XGBoostBestSmallModelFinder[L, G] =
    set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  override def setValidationCol(value: String): XGBoostBestSmallModelFinder[L, G] =
    set(validationCol, value)

  def setLabelCol(label: String): this.type = set(labelCol, label)

  def setGroupByCol(toGroupBy: String): this.type = set(groupByCol, Some(toGroupBy))

  def setIdCol(id: String): this.type = set(idCol, id)

  def setTimeCol(time: String): this.type = set(timeCol, Some(time))

  def setXGBoostParams(params: Map[String, Any]): this.type = set(xGBoostRegLinearParams, params)

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

  override protected def train(dataSet: Dataset[_]): XGBoostSmallModel[G] = {
    val trainSchema = buildTrainSchema(dataSet.sqlContext.sparkContext)
    val idModels = dataSet.rdd.groupBy { case (row: Row) =>
      row.getAs[G]($(groupByCol).get)
    }.map{case f=> train(f._1, f._2.toIterator.asInstanceOf[Iterator[Row]], trainSchema)}
    new XGBoostSmallModel[G](uid, modelEvaluation(idModels))
      .setValidationCol($(validationCol))
      .setIdCol($(idCol))
      .setTimeCol($(timeCol).get)
      .asInstanceOf[XGBoostSmallModel[G]]
  }

  def train(groupedByCol: G,
            rows: Iterator[Row],
            trainSchema: Broadcast[StructType]): (G, Row, Seq[(ParamMap, UberXGBOOSTModel)]) = {

    val (matrixRow, result) = try {
      val array = rows.toArray
      val values = array.map { row =>
        val values = row
          .getAs[org.apache.spark.ml.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
          .toArray
        val label = DataTransformer.toFloat(row.getAs[L]($(labelCol)))
        LabeledPoint.fromDenseVector(label, values.map(_.toFloat))
      }.toIterator
      val valuesVector = array.map { row =>
        val vector =
          row.getAs[org.apache.spark.ml.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
        Vectors.dense(DataTransformer.toDouble(row.getAs($(labelCol))) +: vector.toArray)
      }

      val matrixRow =
        new GenericRowWithSchema(Array(groupedByCol, valuesVector), trainSchema.value)

      val matrix = new DMatrix(values)
      val booster = UberXGBOOSTModel.fitModel(matrix, $(xGBoostRegLinearParams), $(xGBoostRounds))
      (matrixRow,
       Seq((new ParamMap(), new UberXGBOOSTModel($(xGBoostRegLinearParams), $(xGBoostRounds), booster))))
    } catch {
      case e: Exception =>
        log.error(
          s"Got the following Exception ${e.getLocalizedMessage} when doing XGBoost " +
            s"in id $groupedByCol")
        (Row(groupedByCol, Iterator.empty), Seq.empty[(ParamMap, UberXGBOOSTModel)])
    }
    (groupedByCol, matrixRow, result)
  }
}

object XGBoostBestSmallModelFinder
    extends DefaultParamsReadable[XGBoostBestSmallModelFinder[_, _]] {
  override def load(path: String): XGBoostBestSmallModelFinder[_, _] = super.load(path)
}

class XGBoostTrainingSummary[G](predictions: DataFrame,
                                predictionCol: String,
                                labelCol: String,
                                model: XGBoostSmallModel[G],
                                diagInvAtWA: Array[Double],
                                val featuresCol: String,
                                val objectiveHistory: Array[Double])
    extends XGBoostLinearSummary[G](predictions, predictionCol, labelCol, model, diagInvAtWA)
