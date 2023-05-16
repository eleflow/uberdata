/*
 *  Copyright 2015 eleflow.com.br.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.cloudera.sparkts.models

import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.{LabeledPoint => XGBLabeledPoint}
import ml.dmlc.xgboost4j.scala.spark.{XGBoostRegressionModel, XGBoostRegressor}
import org.apache.spark.TaskContext
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * Created by dirceu on 25/08/16.
  */
object UberXGBoostRegressionModel {
  def train(trainLabel: Dataset[LabeledPoint],
            configMap: Map[String, Any],
            round: Int,
            nWorkers: Int): XGBoostRegressionModel = {
//    val trainData = trainLabel.cache
//    XGBoost.trainWithRDD(trainData, configMap, round, nWorkers, useExternalMemory = true, missing
//      = Float.NaN)
    new XGBoostRegressor(configMap).fit(trainLabel)
  }

  def labelPredict(testSet: RDD[XGBLabeledPoint],
                   useExternalCache: Boolean,
                   booster: XGBoostRegressionModel): RDD[(Float, Float)] = {
    val broadcastBooster = testSet.sparkContext.broadcast(booster)
    testSet.mapPartitions { testData =>
      val (toPredict, toLabel) = testData.duplicate
      val dMatrix = new DMatrix(toPredict)
      val prediction = broadcastBooster.value.booster.predict(dMatrix).flatten.toIterator
      toLabel.map(_.label).zip(prediction)
    }
  }

  def labelPredict(testSet: Dataset[_],
                   booster: XGBoostRegressionModel) = {
    val broadcastBooster = testSet.rdd.sparkContext.broadcast(booster)
    val rdd = testSet.cache
    val ret = broadcastBooster.value.transform(testSet).map(value => (value(0),
      value(1)))
    ret
//    testSet.
//    testSet.mapPartitions { testData =>
//      val (toPredict, toLabel) = testData.duplicate
//      val dMatrix = new DMatrix(toPredict)
//
//      val prediction = broadcastBooster.value.booster.predict(dMatrix).flatten.toIterator
//      toLabel.map(_.label).zip(prediction)
//    }
  }
}
