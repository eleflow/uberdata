package org.apache.spark.ml

import org.apache.spark.ml.param.shared.{HasAlpha, HasBeta, HasGamma}

/**
  * Created by dirceu on 17/05/16.
  */
trait HoltWintersParams extends PredictorParams with HasAlpha with HasGamma with HasBeta


