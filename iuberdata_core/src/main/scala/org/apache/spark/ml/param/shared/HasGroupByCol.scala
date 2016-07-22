package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 22/07/16.
  */
trait HasGroupByCol extends Params {

    final val groupByCol: Param[String] = new Param[String](this, "groupBycol", "column name that group small models")

    /** @group getParam */
    final def getGroupByCol: String = $(groupByCol)
  }