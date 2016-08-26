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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{
  MultivariateOnlineSummarizer,
  MultivariateStatisticalSummary
}

/**
  * Created by dirceu on 11/05/16.
  */
class TimeSeriesSmallModelRegressionMetrics(
  idPredictionsAndObservations: Array[(Double, Double)]
) {

  private lazy val summary: MultivariateStatisticalSummary =
    idPredictionsAndObservations.map {
      case (observation, prediction) =>
        Vectors.dense(observation, observation - prediction)
    }.aggregate(new MultivariateOnlineSummarizer())(
      (summary, current) => summary.add(current),
      (sum1, sum2) => sum1.merge(sum2)
    )

  private lazy val SSerr = math.pow(summary.normL2(1), 2)
  private lazy val SStot = summary.variance(0) * (summary.count - 1)
  private lazy val SSreg = {
    val yMean = summary.mean(0)
    idPredictionsAndObservations.map {
      case (prediction, observation) => math.pow(prediction - yMean, 2)
    }.sum
  }

  def explainedVariance = SSreg / summary.count

  def meanAbsoluteError = summary.normL1(1) / summary.count

  def meanSquaredError = SSerr / summary.count

  def rootMeanSquaredPercentageError =
    math.sqrt(idPredictionsAndObservations.map {
      case (observation, prediction) =>
        Math.pow((observation - prediction) / observation, 2)
    }.sum / summary.count)

  def rootMeanSquaredError = math.sqrt(meanSquaredError)

  def r2 = 1 - (SSerr / SStot)

}
