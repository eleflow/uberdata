package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 19/07/16.
  */
trait HasIdCol extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val idCol: Param[String] = new Param[String](this, "IdCol", "id column name")

  /** @group getParam */
  final def getIdCol: String = $(idCol)
}

