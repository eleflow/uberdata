package org.apache.spark.ml.regression

import org.apache.spark.ml.{ArimaModel}
import org.apache.spark.sql.DataFrame

/**
  * Created by dirceu on 12/04/16.
  */
class ARIMALinearSummary[T](@transient val predictions: DataFrame,
                            val predictionCol: String,
                            val labelCol: String,
                            val model: ArimaModel[T],
                            private val diagInvAtWA: Array[Double]) extends Serializable