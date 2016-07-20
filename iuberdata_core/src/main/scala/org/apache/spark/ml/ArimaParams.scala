package org.apache.spark.ml


import org.apache.spark.ml.param.shared._
/**
  * Created by dirceu on 12/04/16.
  */
trait ArimaParams extends PredictorParams
  with HasSolver
  with HasPredictionCol
  with HasARIMAQ
  with HasARIMAP
  with HasARIMAD
  with HasValidationCol
