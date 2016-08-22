package org.apache.spark.ml

import com.cloudera.sparkts.models.HOLTWintersModel
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

/**
  * Created by dirceu on 02/06/16.
  */
abstract  class HoltWintersBestModelEvaluation[L, M <: ForecastBaseModel[M]](implicit kt: ClassTag[L], ord: Ordering[L] = null)
  extends BestModelFinder[L,M] with HoltWintersParams{

  protected def holtWintersEvaluation(row: Row, model: HOLTWintersModel, broadcastEvaluator: Broadcast[TimeSeriesEvaluator[L]],
                                      id: L): (HOLTWintersModel, ModelParamEvaluation[L]) = {
    val features = row.getAs[org.apache.spark.mllib.linalg.Vector]($(featuresCol))
    log.warn(s"Evaluating forecast for id $id, with parameters alpha ${model.alpha}, beta ${model.beta} and gamma ${model.gamma}")
    val expectedResult = row.getAs[org.apache.spark.mllib.linalg.Vector](partialValidationCol)
    val forecastToBeValidated = Vectors.dense(new Array[Double]($(nFutures)))
    model.forecast(features, forecastToBeValidated).toArray
    val toBeValidated = expectedResult.toArray.zip(forecastToBeValidated.toArray)
    val metric = broadcastEvaluator.value.evaluate(toBeValidated)
    val metricName = broadcastEvaluator.value.getMetricName
    val params = ParamMap().put(ParamPair(gamma,model.gamma),ParamPair(beta,model.beta),ParamPair(alpha,model.alpha))
    (model, new ModelParamEvaluation[L](id, metric, params, Some(metricName), SupportedAlgorithm.HoltWinters))
  }
}
