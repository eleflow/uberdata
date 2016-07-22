package org.apache.spark.ml.regression

import org.apache.spark.ml.XGBoostModel
import org.apache.spark.sql.DataFrame

/**
  * Created by dirceu on 30/06/16.
  */
class XGBoostLinearSummary[T](@transient val predictions: DataFrame,
                              val predictionCol: String,
                              val labelCol: String,
                              val model: XGBoostModel[T],
                              private val diagInvAtWA: Array[Double]) extends Serializable
