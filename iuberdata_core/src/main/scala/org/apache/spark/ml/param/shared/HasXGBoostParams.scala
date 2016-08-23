package org.apache.spark.ml.param.shared

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by dirceu on 07/07/16.
  */
trait HasXGBoostParams extends Params {

  final val xGBoostParams: Param[Map[String, Any]] = new Param[Map[String, Any]](this, "xgboostparams", "XGBoost algorithm parameters")
  final val xGBoostRounds: Param[Int] = new Param[Int](this, "xgboostRounds", "the number of rounds used by XGBoost algorithm")

  setDefault(xGBoostRounds, 2000)

  setDefault(xGBoostParams, Map[String, Any](
    "silent" -> 1
    , "objective" -> "reg:linear"
    , "booster" -> "gbtree"
    , "eta" -> 0.0225
    , "max_depth" -> 26
    , "subsample" -> 0.63
    , "colsample_btree" -> 0.63
    , "min_child_weight" -> 9
    , "gamma" -> 0
    , "eval_metric" -> "rmse"
    , "tree_method" -> "exact"
  ).map(f => (f._1, f._2.asInstanceOf[AnyRef])))

  /** @group getParam */
  final def getXGBoostParams: Map[String, Any] = $(xGBoostParams)
  final def getXGBoostRounds: Int = $(xGBoostRounds)
}
