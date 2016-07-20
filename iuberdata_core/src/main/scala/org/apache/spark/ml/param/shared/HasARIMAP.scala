package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
private[ml] trait HasARIMAP extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val arimaP: Param[Int] = new Param[Int](this, "arimaP", "ARIMA autoregressive order")

  setDefault(arimaP, 0)

  /** @group getParam */
  final def getARIMAP: Int = $(arimaP)
}
