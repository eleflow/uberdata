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
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasGroupByCol, HasIdCol, HasXGBoostParams}
import org.apache.spark.ml.regression.XGBoostLinearSummary
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, FloatType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 30/06/16.
  */
class XGBoostBestModelFinder[L, G](override val uid: String)(implicit gt: ClassTag[G])
  extends BestModelFinder[G, XGBoostModel[G]]
    with DefaultParamsWritable
    with HasXGBoostParams
    with HasIdCol
    with HasGroupByCol
    with TimeSeriesBestModelFinder with Logging {
  def this()(implicit gt: ClassTag[G]) = this(Identifiable.randomUID("xgboost"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[G]) = set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  override def setValidationCol(value: String) = set(validationCol, value)

  def setLabelCol(label: String) = set(labelCol, label)

  def setGroupByCol(toGroupBy: String) = set(groupByCol, toGroupBy)

  def setIdCol(id: String) = set(idCol, id)

  def setXGBoostParams(params: Map[String, Any]) = set(xGBoostParams, params)

  def getOrdering(metricName: String): Ordering[Double] = {
    metricName match {
      case "re" => Ordering.Double.reverse
      case _ => Ordering.Double
    }
  }

  def modelEvaluation(idModels: RDD[(G, Row, Seq[(ParamMap, UberXGBOOSTModel)])]):
  RDD[(G, (UberXGBOOSTModel, Seq[ModelParamEvaluation[G]]))] = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)
    val ordering = TimeSeriesEvaluator.ordering(eval.getMetricName)
    idModels.map {
      case (id, row, models) =>
        val evaluatedModels = models.map {
          case (parameters, model) =>
            (model, xGBoostEvaluation(row, model.boosterInstance, broadcastEvaluator, id, parameters))
        }
        val sorted = evaluatedModels.sortBy(_._2.metricResult)(ordering)
        log.warn(s"best model reach ${sorted.head._2.metricResult}")
        log.warn(s"best model params ${sorted.head._2.params}")

        val (bestModel, _) = sorted.head
        (id, (bestModel, sorted.map(_._2)))
    }
  }


  protected def xGBoostEvaluation(row: Row, model: Booster, broadcastEvaluator: Broadcast[TimeSeriesEvaluator[G]],
                                  id: G, parameters: ParamMap): ModelParamEvaluation[G] = {
    val featuresArray = row.getAs[Array[org.apache.spark.mllib.linalg.Vector]](IUberdataForecastUtil.FEATURES_COL_NAME)
      .map { vec =>
        val values = vec.toArray.map(DataTransformer.toFloat)
        LabeledPoint.fromDenseVector(values.head, values.tail)
      }
    val features = new DMatrix(featuresArray.toIterator)
    log.warn(s"Evaluating forecast for id $id, with xgboost")
    val prediction = model.predict(features).flatten
    val (forecastToBeValidated, _) = prediction.splitAt(featuresArray.length)
    val toBeValidated = featuresArray.zip(forecastToBeValidated)
    val metric = broadcastEvaluator.value.evaluate(toBeValidated.map(f => (f._1.label.toDouble, f._2.toDouble)))
    val metricName = broadcastEvaluator.value.getMetricName
    new ModelParamEvaluation[G](id, metric, parameters, Some(metricName), SupportedAlgorithm.XGBoostAlgorithm)
  }

  override protected def train(dataSet: DataFrame): XGBoostModel[G] = {
    val idModels = dataSet.rdd.groupBy { row =>
      row.getAs[G]($(groupByCol))
    }.map(f => train(f._1, f._2.toIterator))
    new XGBoostModel[G](uid, modelEvaluation(idModels)).setIdCol($(idCol)).setValidationCol($(validationCol))
      .asInstanceOf[XGBoostModel[G]]
  }

  def train(id: G, rows: Iterator[Row]): (G, Row, Seq[(ParamMap, UberXGBOOSTModel)]) = {
    val (matrixRow, result) = try {
      val array = rows.toArray
      val values = array.map { row =>
        val values = row.getAs[org.apache.spark.mllib.linalg.Vector](
          IUberdataForecastUtil.FEATURES_COL_NAME).toArray
        val label = DataTransformer.toFloat(row.getAs[L]($(labelCol)))
        LabeledPoint.fromDenseVector(label, values.map(_.toFloat))
      }.toIterator
      val valuesVector = array.map { row =>
        val vector = row.getAs[org.apache.spark.mllib.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
        Vectors.dense(DataTransformer.toDouble(row.getAs($(labelCol))) +: vector.toArray)
      }
      val schema = StructType(Seq(
        StructField($(groupByCol), FloatType),
        StructField(IUberdataForecastUtil.FEATURES_COL_NAME, ArrayType(new VectorUDT))))
      val matrixRow = new GenericRowWithSchema(Array(id, valuesVector), schema)

      val matrix = new DMatrix(values)
      val booster = UberXGBOOSTModel.fitModel(matrix, $(xGBoostParams), $(xGBoostRounds))
      (matrixRow, Seq((new ParamMap(), new UberXGBOOSTModel($(xGBoostParams), $(xGBoostRounds), booster))))
    } catch {
      case e: Exception =>
        log.error(s"Got the following Exception ${e.getLocalizedMessage} when doing XGBoost " +
          s"in id $id")
        (Row(id, Iterator.empty), Seq.empty[(ParamMap, UberXGBOOSTModel)])
    }
    (id, matrixRow, result)
  }
}

object XGBoostBestModelFinder extends DefaultParamsReadable[XGBoostBestModelFinder[_, _]] {

  override def load(path: String): XGBoostBestModelFinder[_, _] = super.load(path)
}


class XGBoostTrainingSummary[G](predictions: DataFrame,
                                predictionCol: String,
                                labelCol: String,
                                model: XGBoostModel[G],
                                diagInvAtWA: Array[Double],
                                val featuresCol: String,
                                val objectiveHistory: Array[Double])
  extends XGBoostLinearSummary[G](predictions, predictionCol, labelCol, model, diagInvAtWA)
