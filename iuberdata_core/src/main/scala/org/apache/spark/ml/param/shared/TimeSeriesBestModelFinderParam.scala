package org.apache.spark.ml.param.shared

import org.apache.spark.ml.TimeSeriesEstimator
import org.apache.spark.ml.evaluation.KeyValueEvaluator
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.tuning.ValidatorParams

/**
  * Created by dirceu on 25/04/16.
  */
trait TimeSeriesBestModelFinderParam[T] extends ValidatorParams {
  /**
    * Param for ratio between train and validation data. Must be between 0 and 1.
    * Default: 0.75
    *
    * @group param
    */

  val keyValueEvaluator: Param[KeyValueEvaluator[T]] = new Param(this, "keyValueEvaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")


  /**
    * param for the estimator to be validated
    *
    * @group param
    */
  val timeSeriesEstimator: Param[TimeSeriesEstimator[T, _]] = new Param(this, "timeSeriesEstimator", "timeseries estimator for selection")

  /** @group getParam */
  def getTimeSeriesEstimator: TimeSeriesEstimator[T, _] = $(timeSeriesEstimator)

  def getKeyValueEvaluator: KeyValueEvaluator[T] = $(keyValueEvaluator)


}

