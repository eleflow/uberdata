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

