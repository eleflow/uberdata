package org.apache.spark.ml.param.shared

import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 12/05/16.
  */
trait HasEvaluator extends Params {

  val evaluator: Param[Evaluator] =
    new Param(this, "evaluator", "evaluator to the used algorithm")

  def getEvaluator = $(evaluator)
}
