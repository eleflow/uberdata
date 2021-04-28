/*
 * Copyright 2015 eleflow.com.br.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eleflow.uberdata.models

import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import ml.dmlc.xgboost4j.{LabeledPoint => XGBLabeledPoint}

import org.apache.spark.rdd.RDD

/**
  * Created by dirceu on 29/06/16.
  */
object UberXGBOOSTModel {
  def fitModel(matrix: DMatrix, params: Map[String, Any], rounds: Int): Booster = {
    XGBoost.train(matrix, params, rounds, Map[String, DMatrix](), null, null)
  }

  def fitSparkModel(matrix: RDD[XGBLabeledPoint],
                    params: Map[String, Any],
                    rounds: Int): RDD[Booster] = {
    matrix.mapPartitions { partitionData =>
      Iterator(XGBoost.train(new DMatrix(partitionData), params, rounds))
    }
  }

  def returnValue[T](value: Option[Any]): T = value.map(f => f.asInstanceOf[T]).get
}

case class UberXGBOOSTModel(objective: String,
                            booster: String,
                            eta: Double,
                            maxDepth: Int,
                            subSample: Double,
                            colSampleBTree: Double,
                            minChildWeight: Int,
                            gamma: Int,
                            evalMetric: String,
                            treeMethod: String,
                            rounds: Int,
                            boosterInstance: Booster) {

  def this(parameters: Map[String, Any], rounds: Int, boosterInstance: Booster) =
    this(
      UberXGBOOSTModel.returnValue[String](parameters.get("objective")),
      UberXGBOOSTModel.returnValue[String](parameters.get("booster")),
      UberXGBOOSTModel.returnValue[Double](parameters.get("eta")),
      UberXGBOOSTModel.returnValue[Int](parameters.get("max_depth")),
      UberXGBOOSTModel.returnValue[Double](parameters.get("subsample")),
      UberXGBOOSTModel.returnValue[Double](parameters.get("colsample_btree")),
      UberXGBOOSTModel.returnValue[Int](parameters.get("min_child_weight")),
      UberXGBOOSTModel.returnValue[Int](parameters.get("gamma")),
      UberXGBOOSTModel.returnValue[String](parameters.get("eval_metric")),
      UberXGBOOSTModel.returnValue[String](parameters.get("tree_method")),
      rounds,
      boosterInstance)

  lazy val params = Map(
    "objective" -> objective,
    "booster" -> booster,
    "eta" -> eta,
    "maxDepth" -> maxDepth,
    "subSample" -> subSample,
    "colSampleBTree" -> colSampleBTree,
    "minChildWeight" -> minChildWeight,
    "gamma" -> gamma,
    "evalMetric" -> evalMetric,
    "treeMethod" -> treeMethod
  )
}
