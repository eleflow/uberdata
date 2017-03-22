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

package org.apache.spark.ml

import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.core.data.DataTransformer
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 29/06/16.
  */
class XGBoost[I](override val uid: String,
                 val models: RDD[(I, (UberXGBOOSTModel,
                   Seq[(ModelParamEvaluation[I])]))])(
  implicit kt: ClassTag[I],
  ord: Ordering[I] = null)
    extends ForecastBaseModel[XGBoostSmallModel[I]]
    with HasInputCol
    with HasOutputCol
    with DefaultParamsWritable
    with HasFeaturesCol
    with HasNFutures
    with HasGroupByCol {

  def this(
    models: RDD[(I, (UberXGBOOSTModel, Seq[(ModelParamEvaluation[I])]))]
  )(implicit kt: ClassTag[I], ord: Ordering[I] ) =
    this(Identifiable.randomUID("xgboost"), models)

  override def transform(dataSet: Dataset[_]): DataFrame = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema)

    val joined = models.join(dataSet.rdd.map{case (r: Row) => (r.getAs[I]($(groupByCol).get), r)})

    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row.getAs[Array[org.apache.spark.ml.linalg.Vector]](
          IUberdataForecastUtil.FEATURES_COL_NAME
        )
        val label = DataTransformer.toFloat(row.getAs($(featuresCol)))
        val labelPoint = features.map { vec =>
          val array = vec.toArray.map(_.toFloat)
          LabeledPoint.fromDenseVector(label, array)
        }
        val matrix = new DMatrix(labelPoint.toIterator)
        val (ownFeaturesPrediction, forecast) = bestModel.boosterInstance
          .predict(matrix)
          .flatMap(_.map(_.toDouble))
          .splitAt(features.length)
        Row(
          row.toSeq :+ Vectors
            .dense(forecast) :+ SupportedAlgorithm.XGBoostAlgorithm.toString :+ bestModel.params
            .map(f => f._1 -> f._2.toString) :+ Vectors.dense(ownFeaturesPrediction): _*
        )
    }
    dataSet.sqlContext.createDataFrame(predictions, predSchema)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField($(outputCol), ArrayType(DoubleType)))
  }

  override def copy(extra: ParamMap): XGBoostSmallModel[I] = defaultCopy(extra)
}
