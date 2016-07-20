package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
trait HasTimeCol extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val timeCol: Param[String] = new Param[String](this, "timeCol", "time series column name")

  setDefault(timeCol, "date")

  /** @group getParam */
  final def getTimeCol: String = $(timeCol)
}
