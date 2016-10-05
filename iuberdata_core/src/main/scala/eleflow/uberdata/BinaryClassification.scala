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

package eleflow.uberdata

import eleflow.uberdata.enums.SupportedAlgorithm._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, TimeSeriesEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.shared.HasXGBoostParams


import scala.reflect.ClassTag

/**
  * Created by caio.martins on 22/09/16.
  */

object BinaryClassification {
  def apply(): BinaryClassification = new BinaryClassification
}

class BinaryClassification {

  def predict(
               train: DataFrame,
               test: DataFrame,
               algorithm: Algorithm,
               labelCol: String,
               idCol: String,
               featuresCol: Seq[String],
               rounds: Int = 2000,
               params: Map[String, Any] = Map.empty[String, Any]): (DataFrame, PipelineModel) = {
    val pipeline = algorithm match {
      case XGBoostAlgorithm =>
        prepareXGBoostBigModel(labelCol, idCol, featuresCol, train.schema, rounds, params)
      case _ => throw new UnsupportedOperationException()
    }
    val model = pipeline.fit(train.cache)
    val predictions = model.transform(test).cache
    (predictions.sort(idCol), model)
  }

  def prepareXGBoostBigModel[L, G](
                                    labelCol: String,
                                    idCol: String,
                                    featuresCol: Seq[String],
                                    schema: StructType,
                                    rounds: Int,
                                    params: Map[String, Any])(implicit ct: ClassTag[L], gt: ClassTag[G]): Pipeline = {

    val xgboost = new XGBoostBestBigModelFinder[L, G]()
      .setLabelCol(labelCol)
      .setIdCol(idCol)

    new Pipeline().setStages(
      createXGBoostPipelineStages(labelCol, featuresCol, Some(idCol), schema = schema) :+ xgboost)
  }

  def createXGBoostPipelineStages(labelCol: String,
                                  featuresCol: Seq[String],
                                  idCol: Option[String] = None,
                                  schema: StructType): Array[PipelineStage] = {

    val allColumns = schema.map(_.name).toArray

    val stringColumns = schema
      .filter(f => f.dataType.isInstanceOf[StringType] && featuresCol.contains(f.name))
      .map(_.name)

    val nonStringColumns = allColumns.filter(
      f =>
        !stringColumns.contains(f)
          && featuresCol.contains(f))


    val stringIndexers = stringColumns.map { column =>
      new StringIndexer().setInputCol(column).setOutputCol(s"${column}Index")
    }.toArray

    val encoder = stringColumns.map { column =>
      new OneHotEncoder().setInputCol(s"${column}Index").setOutputCol(s"${column}Encoder")
    }.toArray

    val nonStringIndex = "nonStringIndex"
    val columnIndexers = new VectorizeEncoder()
      .setInputCol(nonStringColumns)
      .setOutputCol(nonStringIndex)
      .setLabelCol(labelCol)
      .setIdCol(idCol.getOrElse(""))

    val assembler = new VectorAssembler()
      .setInputCols(stringColumns.map(f => s"${f}Index").toArray :+ nonStringIndex)
      .setOutputCol(IUberdataForecastUtil.FEATURES_COL_NAME)

    stringIndexers ++ encoder :+ columnIndexers :+ assembler
  }
}