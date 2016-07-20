package org.apache.spark.ml.param.shared

import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 11/05/16.
  */
trait HasTimeSeriesEvaluator[T] extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val timeSeriesEvaluator: Param[TimeSeriesEvaluator[T]] = new Param[TimeSeriesEvaluator[T]](this,
    "timeSeriesEvaluator", "Evaluator to be used to validate the models")

  /** @group getParam */
  final def getTimeSeriesEvaluator: TimeSeriesEvaluator[T] = $(timeSeriesEvaluator)
}

