///*
// *  Copyright 2015 eleflow.com.br.
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *  http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */
//
//package org.apache.spark.ml
//
//
//import com.cloudera.sparkts.models.UberXGBoostModel
//import eleflow.uberdata.IUberdataForecastUtil
//import eleflow.uberdata.core.data.DataTransformer
//import eleflow.uberdata.enums.SupportedAlgorithm
//import ml.dmlc.xgboost4j.scala.spark.{XGBoostRegressionModel,XGBoostClassificationModel}
//import ml.dmlc.xgboost4j.LabeledPoint
//import org.apache.spark.annotation.DeveloperApi
//import org.apache.spark.ml.linalg.{VectorUDT, Vector => SparkVector}
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.feature.{LabeledPoint => SparkLabeledPoint}
//import org.apache.spark.ml.param.shared.{HasIdCol, HasLabelCol}
//import org.apache.spark.sql.{DataFrame, Row}
//import org.apache.spark.sql.Dataset
//import org.apache.spark.sql.types.{StructField, _}
//
///**
//  * Created by dirceu on 24/08/16.
//  */
//class XGBoostBigModel[I](val uid: String, val models: Seq[(ParamMap, XGBoostModel)])
//    extends ForecastBaseModel[XGBoostBigModel[I]]
//    with HasLabelCol
//    with HasIdCol {
//
//  def setLabelcol(label: String): this.type = set(labelCol, label)
//
//  def setIdcol(id: String): this.type = set(idCol, id)
//
//  override def copy(extra: ParamMap): XGBoostBigModel[I] = new XGBoostBigModel[I](uid, models)
//
//  override def transform(dataSet: Dataset[_]): DataFrame = {
//    val prediction = predict(dataSet)
//    val rows = dataSet.rdd
//      .map {
//        case (row: Row) =>
//          (DataTransformer.toFloat(row.getAs($(idCol))),
//            row.getAs[SparkVector](IUberdataForecastUtil.FEATURES_COL_NAME)
//            )
//      }
//      .join(prediction)
//      .map {
//        case (id, (features, predictValue)) =>
//          Row(id, features, SupportedAlgorithm.XGBoostAlgorithm.toString, predictValue)
//      }
//    dataSet.sqlContext.createDataFrame(rows, transformSchema(dataSet.schema))
//  }
//
//  protected def predict(dataSet: Dataset[_]) = {
//    val features = dataSet.rdd.map { case (row: Row) =>
//      val features = row.getAs[SparkVector](IUberdataForecastUtil.FEATURES_COL_NAME)
//      val id = row.getAs[I]($(idCol))
//      SparkLabeledPoint(DataTransformer.toFloat(id), features)
//    }.cache
//    val (_, model) = models.head
//    UberXGBoostModel.labelPredict(features.map(_.features.toDense), booster = model)
//  }
//
//  @DeveloperApi
//  override def transformSchema(schema: StructType): StructType =
//    StructType(getPredictionSchema)
//
//  protected def getPredictionSchema: Array[StructField] = {
//    Array(
//      StructField($(idCol), FloatType),
//      StructField(IUberdataForecastUtil.FEATURES_COL_NAME, new VectorUDT),
//      StructField(IUberdataForecastUtil.ALGORITHM, StringType),
//      StructField("prediction", FloatType)
//    )
//  }
//}
