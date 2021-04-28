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

import com.cloudera.sparkts.models.TimeSeriesModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/**
  * Created by dirceu on 25/04/16.
  * Chama o bestmodelfinder para achar o melhor modelo
  * e retreina o dataset com todos os dados no melhor modelo
  */
class ArimaBestModel[L, M <: TimeSeriesModel](
  override val uid: String,
  val bestPrediction: RDD[(L, M)],
  val validationMetrics: RDD[(L, Seq[ModelParamEvaluation[L]])]
) extends Model[ArimaBestModel[L, M]]
    with TimeSeriesBestModelFinderParam[L] {

  //TODO avaliar necessidade
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    dataset.toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): ArimaBestModel[L, M] = {
    val copied =
      new ArimaBestModel[L, M](uid, bestPrediction, validationMetrics)
    copyValues(copied, extra)
  }
}
