package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, ParamMap, Params}

/**
  * Created by dirceu on 10/05/16.
  */
trait HasEstimatorParams extends Params {

  val estimatorParamMaps: Param[Array[ParamMap]] =
    new Param(this, "estimatorParamMaps", "param maps for the estimator")

  def getEstimatorParamMaps = $(estimatorParamMaps)
}

