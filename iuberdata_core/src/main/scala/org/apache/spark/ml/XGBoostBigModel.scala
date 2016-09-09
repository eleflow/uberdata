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

import java.sql.Timestamp

import com.cloudera.sparkts.models.UberXGBoostModel
import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.core.data.DataTransformer
import eleflow.uberdata.enums.SupportedAlgorithm
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import ml.dmlc.xgboost4j.LabeledPoint
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{VectorUDT, Vector => SparkVector}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasIdCol, HasLabelCol, HasTimeCol}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, _}

/**
  * Created by dirceu on 24/08/16.
  */
class XGBoostBigModel[I](val uid: String, val models: Seq[(ParamMap, XGBoostModel)])
    extends ForecastBaseModel[XGBoostBigModel[I]]
    with HasLabelCol
    with HasIdCol
    with HasTimeCol{

  def setLabelcol(label: String): this.type = set(labelCol, label)

  def setIdcol(id: String): this.type = set(idCol, id)

  def setTimecol(time: String): this.type = set(timeCol, time)

  override def copy(extra: ParamMap): XGBoostBigModel[I] = new XGBoostBigModel[I](uid, models)

  override def transform(dataSet: DataFrame): DataFrame = {
    val prediction = predict(dataSet)
    val rows = dataSet
      .map(
        row =>
            (DataTransformer.toFloat(row.getAs($(idCol))),
              (row.getAs[SparkVector](IUberdataForecastUtil.FEATURES_COL_NAME),
                row.getAs[Timestamp]($(timeCol))))
      )
      .join(prediction)
      .map {
        case (id, ((features, time), predictValue)) =>
            Row(id, features, time, SupportedAlgorithm.XGBoostAlgorithm.toString, predictValue)
      }
    dataSet.sqlContext.createDataFrame(rows, transformSchema(dataSet.schema))
  }

  private def predict(dataSet: DataFrame) = {
    val features = dataSet.map { row =>
      val features = row.getAs[SparkVector](IUberdataForecastUtil.FEATURES_COL_NAME)
      val id = row.getAs[I]($(idCol))
      LabeledPoint.fromDenseVector(DataTransformer.toFloat(id), features.toArray.map(_.toFloat))
    }
    val (_, model) = models.head
    UberXGBoostModel.labelPredict(features, booster = model)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    StructType(
      Array(
        StructField($(idCol), FloatType),
        StructField(IUberdataForecastUtil.FEATURES_COL_NAME, new VectorUDT),
        StructField($(timeCol), TimestampType),
        StructField(IUberdataForecastUtil.ALGORITHM, StringType),
        StructField("prediction", FloatType)
        ))
}
