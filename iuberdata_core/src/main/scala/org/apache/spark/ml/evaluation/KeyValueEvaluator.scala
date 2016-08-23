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
