package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 02/06/16.
  */
private[ml] trait HasWindowParams extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val windowParams: Param[Seq[Int]] = new Param[Seq[Int]](this, "windowParams", "Moving average window params to be used.")

  final val windowParam: Param[Int] = new Param[Int](this, "windowParam", "Moving average window param in use.")

  /** @group getParam */
  final def getWindowParams: Seq[Int] = $(windowParams)
}
