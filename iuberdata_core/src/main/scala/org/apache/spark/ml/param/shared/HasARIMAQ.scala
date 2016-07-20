package org.apache.spark.ml.param.shared


import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
private[ml] trait HasARIMAQ extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val arimaQ: Param[Int] = new Param[Int](this, "arimaQ", "ARIMA differencing order")

  setDefault(arimaQ, 0)

  /** @group getParam */
  final def getARIMAQ: Int = $(arimaQ)
}