package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 28/04/16.
  */
trait HasNFutures extends Params {

  final val nFutures: Param[Int] = new Param[Int](this, "nFutures", "number of futures predictions")

  setDefault(nFutures, 1)

  /** @group getParam */
  final def getNFutures: Int = $(nFutures)

  def setNFutures(value: Int) = set(nFutures, value)
}