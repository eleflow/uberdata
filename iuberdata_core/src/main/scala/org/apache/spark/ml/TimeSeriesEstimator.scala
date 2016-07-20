package org.apache.spark.ml

import org.apache.spark.ml.param.shared.HasEstimatorParams


/**
  * Created by dirceu on 04/05/16.
  */
trait TimeSeriesEstimator[T,M <: Model[M]] extends Estimator[M]
with HasEstimatorParams{

}

