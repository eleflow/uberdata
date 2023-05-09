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
//import java.sql.Timestamp
//
//import eleflow.uberdata.IUberdataForecastUtil
//import eleflow.uberdata.core.data.DataTransformer
//import eleflow.uberdata.enums.SupportedAlgorithm
//import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
//import org.apache.spark.annotation.DeveloperApi
//import org.apache.spark.ml.linalg.{VectorUDT, Vector => SparkVector}
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.param.shared.HasTimeCol
//import org.apache.spark.sql.{DataFrame, Row}
//import org.apache.spark.sql.Dataset
//import org.apache.spark.sql.types.{StructField, _}
//
///**
//  * Created by caio on 27/09/16.
//  */
//class XGBoostBigModelTimeSeries[I](override val uid: String,
//                                   override val models: Seq[(ParamMap, XGBoostModel)])
//                                  extends XGBoostBigModel[I](uid, models) with HasTimeCol{
//
//  def setTimecol(time: String): this.type = set(timeCol, Some(time))
//
//  override def transform(dataSet: Dataset[_]): DataFrame = {
//    val prediction = predict(dataSet)
//    val rows = dataSet.rdd
//      .map {
//        case (row: Row) =>
//          (DataTransformer.toFloat(row.getAs($(idCol))),
//            (row.getAs[SparkVector](IUberdataForecastUtil.FEATURES_COL_NAME),
//              row.getAs[java.sql.Timestamp]($(timeCol).get)))
//      }
//      .join(prediction)
//      .map {
//        case (id, ((features, time), predictValue)) =>
//          Row(id, features, time, SupportedAlgorithm.XGBoostAlgorithm.toString, predictValue)
//      }
//    dataSet.sqlContext.createDataFrame(rows, transformSchema(dataSet.schema))
//  }
//
//
//  @DeveloperApi
//  override def transformSchema(schema: StructType): StructType =
//    StructType(Array(
//      StructField($(idCol), FloatType),
//      StructField(IUberdataForecastUtil.FEATURES_COL_NAME, new VectorUDT),
//      StructField($(timeCol).get, TimestampType),
//      StructField(IUberdataForecastUtil.ALGORITHM, StringType),
//      StructField("prediction", FloatType)
//    ) )
//}
