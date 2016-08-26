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

import ml.dmlc.xgboost4j.java.{DMatrix => JDMatrix}
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import ml.dmlc.xgboost4j.scala.spark.{DataUtils, XGBoostModel}
import ml.dmlc.xgboost4j.LabeledPoint
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Created by dirceu on 25/08/16.
 */
class UberXGBoostModel(_booster: Booster)(implicit sc: SparkContext)
	extends XGBoostModel(_booster) {
//	def predict(testSet: RDD[LabeledPoint], useExternalCache: Boolean = false)
//	:	RDD[Array[Array[Float]]]	= {
////		import DataUtils._
//		val broadcastBooster = testSet.sparkContext.broadcast(_booster)
//		val appName = testSet.context.appName
//		testSet.mapPartitions { testSamples =>
//			if (testSamples.hasNext) {
//				val cacheFileName = {
//					if (useExternalCache) {
//						s"$appName-dtest_cache-${TaskContext.getPartitionId()}"
//					} else {
//						null //scalastyle:ignore
//					}
//				}
//				val dMatrix = new DMatrix(testSamples, cacheFileName)
//				Iterator(broadcastBooster.value.predict(dMatrix))
//			} else {
//				Iterator()
//			}
//		}
//	}
}
