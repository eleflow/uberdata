package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params, _}

/**
  * Created by dirceu on 17/05/16.
  */
private[ml] trait HasBeta extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val beta: Param[Double] = new Param[Double](this, "beta", "Holt Winters beta param")

  setDefault(beta, 0d)

  /** @group getParam */
  final def getBeta: Double = $(beta)
}
