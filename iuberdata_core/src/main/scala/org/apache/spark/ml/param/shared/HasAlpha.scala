package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
private[ml] trait HasAlpha extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val alpha: Param[Double] = new Param[Double](this, "alpha", "Holt Winters alpha param")

  setDefault(alpha, 0d)

  /** @group getParam */
  final def getAlpha: Double = $(alpha)
}