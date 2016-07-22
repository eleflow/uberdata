package org.apache.spark.ml.evaluation

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, MultivariateStatisticalSummary}


/**
  * Created by dirceu on 11/05/16.
  */
class TimeSeriesSmallModelRegressionMetrics(idPredictionsAndObservations: Array[(Double, Double)]) {

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

  def rootMeanSquaredPercentageError = math.sqrt(idPredictionsAndObservations.map {
    case (observation, prediction) => if (observation == 0) {
      0
    } else {
      Math.pow((observation - prediction) / observation, 2)
    }
  }.sum / summary.count)

  def rootMeanSquaredError = math.sqrt(meanSquaredError)

  def r2 = 1 - (SSerr / SStot)

}



