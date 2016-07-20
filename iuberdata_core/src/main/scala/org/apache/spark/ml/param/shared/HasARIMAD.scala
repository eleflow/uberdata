package org.apache.spark.ml.param.shared


import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
private[ml] trait HasARIMAD extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val arimaD: Param[Int] = new Param[Int](this, "arimaD", "ARIMA MA order")

  setDefault(arimaD, 0)

  /** @group getParam */
  final def getARIMAD: Int = $(arimaD)
}