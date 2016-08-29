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

package org.apache.spark.ml

import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.core.data.DataTransformer
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import ml.dmlc.xgboost4j.LabeledPoint
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasIdCol, HasLabelCol}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
	* Created by dirceu on 24/08/16.
	*/
class XGBoostBigModel[I](val uid: String, val models: Seq[(ParamMap, XGBoostModel)])
    extends ForecastBaseModel[XGBoostBigModel[I]]
    with HasLabelCol
    with HasIdCol {

  def setLabelcol(label: String): this.type = set(labelCol, label)

  def setIdcol(id: String): this.type = set(idCol, id)

  override def copy(extra: ParamMap): XGBoostBigModel[I] = new XGBoostBigModel[I](uid, models)

  override def transform(dataset: DataFrame): DataFrame = {
    val features = dataset.map { row =>
      val features = row.getAs[SparkVector](IUberdataForecastUtil.FEATURES_COL_NAME)
      val id = row.getAs[I]($(idCol))
      LabeledPoint.fromDenseVector(DataTransformer.toFloat(id), features.toArray.map(_.toFloat))
    }
    val (params, model) = models.head

    val x = features.collect.toIterator
    val prediction = model.predict(new DMatrix(features.collect.toIterator))
    dataset
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???
}
