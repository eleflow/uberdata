package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 26/05/16.
  */
trait HasLastValuesParam extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val lastValues: Param[Int] = new Param[Int](this, "lastValues", "Number of values to be in the output")

  setDefault(lastValues, 0)

  /** @group getParam */
  final def getLastValues: Int = $(lastValues)
}

