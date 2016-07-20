package org.apache.spark.ml.param.shared

/**
  * Created by dirceu on 25/04/16.
  */

import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
trait SplitValue extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val splitValue: Param[String] = new Param[String](this, "splitValue", "Value to be used to split train and " +
    "validation dataset")

  /** @group getParam */
  final def getSplitValue: String = $(splitValue)
}
