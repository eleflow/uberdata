package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 28/04/16.
  */
trait HasValidationCol extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val validationCol: Param[String] = new Param[String](this, "validationCol", "validation column name")

  setDefault(validationCol, "validation")

  /** @group getParam */
  final def getValidationCol: String = $(validationCol)
}

