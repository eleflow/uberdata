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
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by dirceu on 25/08/16.
  */
object UberXGBoostModel {
  def train(trainLabel: RDD[LabeledPoint],
            configMap: Map[String, Any],
            round: Int,
            nWorkers: Int): XGBoostModel = {
    XGBoost.train(trainLabel, configMap, round, nWorkers)
  }

  def labelPredict(testSet: RDD[XGBLabeledPoint],
                   useExternalCache: Boolean = false,
                   booster: XGBoostModel): RDD[(Float, Float)] = {
    val broadcastBooster = testSet.sparkContext.broadcast(booster)
    testSet.mapPartitions { testData =>
      val (auxiliaryIterator, testDataIterator) = testData.duplicate
      val testDataArray = auxiliaryIterator.toArray
      val prediction = broadcastBooster.value.booster.predict(new DMatrix(testDataIterator)).flatten
      testDataArray
        .zip(prediction)
        .map {
          case (labeledPoint, predictionValue) =>
            (labeledPoint.label, predictionValue)
        }
        .toIterator
    }
  }
}
