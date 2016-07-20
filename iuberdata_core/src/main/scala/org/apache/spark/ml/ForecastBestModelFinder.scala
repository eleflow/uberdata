package org.apache.spark.ml

import com.cloudera.sparkts.models._
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.ml.param.shared.{HasTimeSeriesEvaluator, HasWindowParams}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 31/05/16.
  */
class ForecastBestModelFinder[T, M <: ForecastBaseModel[M]](override val uid: String)(implicit kt: ClassTag[T])
  extends HoltWintersBestModelEvaluation[T, M]
    with HoltWintersParams with DefaultParamsWritable
    with HasWindowParams
    with ArimaParams
    with HasTimeSeriesEvaluator[T]
    with TimeSeriesBestModelFinder
    with Logging {

  def this() (implicit kt: ClassTag[T]) = this(Identifiable.randomUID("BestForecast"))

  def setWindowParams(params:Seq[Int]) = set(windowParams,params)

  def movingAverageEvaluation(row: Row, model: HOLTWintersModel, broadcastEvaluator: Broadcast[TimeSeriesEvaluator[T]],
                              id: T) = {
    val features = row.getAs[org.apache.spark.mllib.linalg.Vector]($(featuresCol))
    log.warn(s"Evaluating forecast for id $id, with parameters alpha ${model.alpha}, beta ${model.beta} and gamma ${model.gamma}")
    val expectedResult = row.getAs[org.apache.spark.mllib.linalg.Vector](partialValidationCol)
    val forecastToBeValidated  = Vectors.dense(new Array[Double]($(nFutures)))
    model.forecast(features, forecastToBeValidated).toArray
    $(windowParams).map { windowSize =>
      val movingAverageToBeValidate = MovingAverageCalc.simpleMovingAverageArray(forecastToBeValidated.toArray, windowSize)
      val toBeValidated = expectedResult.toArray.zip(movingAverageToBeValidate)
      val metric = broadcastEvaluator.value.evaluate(toBeValidated)
      val metricName = broadcastEvaluator.value.getMetricName
      val params = ParamMap().put(new ParamPair[Int](windowParam,windowSize))
      (model, new ModelParamEvaluation[T](id, metric, params, Some(metricName),SupportedAlgorithm.MovingAverage8))
    }
  }

  def modelEvaluation(idModels: RDD[(T, Row, Seq[(ParamMap, TimeSeriesModel)])]) = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)

    val ordering = TimeSeriesEvaluator.ordering(eval.getMetricName)
    idModels.map {
      case (id, row, models) =>
        val evaluatedModels = models.flatMap {
          case (parameters, model: UberArimaModel) =>
            Seq(arimaEvaluation(row, model, broadcastEvaluator, id, parameters))
          case (parameters, model: HOLTWintersModel) =>
            Seq(holtWintersEvaluation(row, model, broadcastEvaluator, id)) ++
              movingAverageEvaluation(row, model, broadcastEvaluator, id)
        }
        val sorted = evaluatedModels.sortBy(_._2.metricResult)(ordering)
        log.warn(s"best model reach ${sorted.head._2.metricResult}")
        log.warn(s"best model was ${sorted.head._2.algorithm}")
        log.warn(s"best model params ${sorted.head._2.params}")

        val (bestModel, _) = sorted.head
        (id, (bestModel, sorted))
    }
  }

  override protected def train(dataSet: DataFrame): M = {
    val splitDs = split(dataSet, $(nFutures))
    val idModels = splitDs.rdd.map(train)
    new ForecastBestModel[T](uid, modelEvaluation(idModels)).setValidationCol($(validationCol)).asInstanceOf[M]
  }

  def train(row: Row): (T, Row, Seq[(ParamMap, TimeSeriesModel)]) = {
    val id = row.getAs[T]($(labelCol))

    val holtWintersResults = try {
      val holtWinters = HOLTWinters.fitModel(row.getAs($(featuresCol)), $(nFutures))
      val params = ParamMap().put(ParamPair(alpha, holtWinters.alpha)).put(ParamPair(beta, holtWinters.beta)).put(ParamPair(gamma, holtWinters.gamma))
      Some((params, holtWinters))
    } catch {
      case e: Exception =>
        log.error(s"Got the following Exception ${e.getLocalizedMessage} in id $id")
        None
    }
    val arimaResults =
      $(estimatorParamMaps).map {
      params =>
        val q = params.getOrElse(arimaQ, 0)
        val p = params.getOrElse(arimaP, 0)
        val d = params.getOrElse(arimaD, 0)
        try {
          val result = UberArimaModel.fitModel(p, d, q, row.getAs($(featuresCol)))
          val params = ParamMap(ParamPair(arimaP,result.p),ParamPair(arimaQ,result.q),ParamPair(arimaD,result.d))
          Some(params, result)
        } catch {
          case e: Exception =>
            log.error(s"Got the following Exception ${e.getLocalizedMessage} in id $id")
            None
        }
    }
    (id, row, (Seq(holtWintersResults) ++ arimaResults).flatten)
  }
}
