package org.apache.spark.ml.evaluation

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by dirceu on 03/05/16.
  */
abstract class KeyValueEvaluator[T] extends Params {

  /**
    * Evaluates model output and returns a scalar metric (larger is better).
    *
    * @param dataSet  a dataset that contains labels/observations and predictions.
    * @param paramMap parameter map that specifies the input columns and output metrics
    * @return metric
    */
  @Since("1.5.0")
  def evaluate(dataSet: DataFrame, paramMap: ParamMap): RDD[(T, (Int, Double))] = {
    this.copy(paramMap).evaluate(dataSet)
  }

  /**
    * Evaluates the output.
    *
    * @param dataset a dataset that contains labels/observations and predictions.
    * @return metric
    */
  @Since("1.5.0")
  def evaluate(dataset: DataFrame): RDD[(T, (Int, Double))]

  def evaluate(dataset: (T, (Int, org.apache.spark.mllib.linalg.Vector))): RDD[(T, (Int, Double))]

  /**
    * Indicates whether the metric returned by [[evaluate()]] should be maximized (true, default)
    * or minimized (false).
    * A given evaluator may support multiple metrics which may be maximized or minimized.
    */
  @Since("1.5.0")
  def isLargerBetter: Boolean = true

  @Since("1.5.0")
  override def copy(extra: ParamMap): KeyValueEvaluator[T]

}
