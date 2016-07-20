package org.apache.spark.ml.evaluation

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateOnlineSummarizer, MultivariateStatisticalSummary}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by dirceu on 03/05/16.
  */
class TimeSeriesRegressionMetrics[T](idPredictionsAndObservations: RDD[(T, Int, Array[(Double, Double)])],
                                     isLargerBetter: Boolean)(implicit kt: ClassTag[T], ord: Ordering[T] = null) {

  private lazy val summaryRDD: RDD[(T, Int, Array[(Double, Double)], MultivariateStatisticalSummary)] = idPredictionsAndObservations.map {
    case (id, modelIndex, array) => (id, modelIndex, array, array.map {
      case (observation, prediction) =>
        Vectors.dense(observation, observation - prediction)
    }.aggregate(new MultivariateOnlineSummarizer())(
      (summary, current) => summary.add(current),
      (sum1, sum2) => sum1.merge(sum2)
    ))
  }
  private lazy val SSerr = summaryRDD.map { case (id, modelIndex, values, summary) => ((id, modelIndex), (math.pow(summary.normL2(1), 2), summary)) }
  private lazy val SStot = summaryRDD.map { case (id, modelIndex, values, summary) => ((id, modelIndex), summary.variance(0) * (summary.count - 1)) }
  private lazy val SSreg = {
    summaryRDD.map { case (id, modelIndex, values, summary) =>
      val yMean = summary.mean(0)
      (id, modelIndex, values.map {
        case (prediction, observation) => math.pow(prediction - yMean, 2)
      }.sum, summary)
    }
  }

  def explainedVariance: RDD[(T, (Int, Double))] = SSreg.map {
    case (id, modelIndex, regValue, summary) => (id, (modelIndex, regValue / summary.count))
  }

  def meanAbsoluteError: RDD[(T, (Int, Double))] = summaryRDD.map {
    case (id, modelIndex, _, summary) =>
      (id, (modelIndex, summary.normL1(1) / summary.count))
  }

  def meanSquaredError: RDD[(T, (Int, Double))] = SSerr.map {
    case ((id, modelIndex), (err, summary)) =>
      (id, (modelIndex, err / summary.count))
  }

  def rootMeanSquaredError: RDD[(T, (Int, Double))] = meanSquaredError.map {
    case (id, (modelIndex, err)) =>
      (id, (modelIndex, math.sqrt(err)))
  }

  def r2: RDD[(T, (Int, Double))] = SSerr.join(SStot).map {
    case ((id, modelIndex), ((sSerr, _), (sStot))) =>
      (id, (modelIndex, 1 - calc(f => sSerr / sStot)))
  }

  //TODO refazer
  private def calc(f: Any => Double) = try {
    f()
  } catch {
    case e: Exception =>
      e.printStackTrace()
      if (isLargerBetter) 0d
      else Double.MaxValue
  }
}


