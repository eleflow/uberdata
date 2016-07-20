package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params, _}
/**
  * Created by dirceu on 17/05/16.
  */
private[ml] trait HasGamma extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val gamma: Param[Double] = new Param[Double](this, "gamma", "Holt Winters gamma param")

  setDefault(gamma, 0d)

  /** @group getParam */
  final def getGamma: Double = $(gamma)
}