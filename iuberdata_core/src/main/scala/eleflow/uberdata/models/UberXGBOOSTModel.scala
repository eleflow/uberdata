package eleflow.uberdata.models

import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost, Booster}

/**
  * Created by dirceu on 29/06/16.
  */
object UberXGBOOSTModel {
  def fitModel(matrix: DMatrix, params: Map[String, Any], rounds: Int) = {
    XGBoost.train(matrix, params, rounds, Map[String, DMatrix](), null, null)
  }

  def returnValue[T](value: Option[Any]) = value.map(_.asInstanceOf[T]).get
}

case class UberXGBOOSTModel(objective: String, booster: String, eta: Double, maxDepth: Int, subSample: Double,
                            colSampleBTree: Double, minChildWeight: Int, gamma: Int, evalMetric: String,
                            treeMethod: String, rounds: Int, boosterInstance: Booster) {
  def this(parameters: Map[String, Any], rounds: Int, boosterInstance: Booster) = this(
    UberXGBOOSTModel.returnValue[String](parameters.get("objective")), UberXGBOOSTModel.returnValue[String](parameters.get("booster")),
    UberXGBOOSTModel.returnValue[Double](parameters.get("eta")), UberXGBOOSTModel.returnValue[Int](parameters.get("max_depth")),
    UberXGBOOSTModel.returnValue[Double](parameters.get("subsample")), UberXGBOOSTModel.returnValue[Double](parameters.get("colsample_btree")),
    UberXGBOOSTModel.returnValue[Int](parameters.get("min_child_weight")), UberXGBOOSTModel.returnValue[Int](parameters.get("gamma")),
    UberXGBOOSTModel.returnValue[String](parameters.get("eval_metric")),
    UberXGBOOSTModel.returnValue[String](parameters.get("tree_method")), rounds, boosterInstance)

  lazy val params = Map(
    "objective" -> objective
    , "booster" -> booster
    , "eta" -> eta
    , "maxDepth" -> maxDepth
    , "subSample" -> subSample
    , "colSampleBTree" -> colSampleBTree
    , "minChildWeight" -> minChildWeight
    , "gamma" -> gamma
    , "evalMetric" -> evalMetric
    , "treeMethod" -> treeMethod
  )
}