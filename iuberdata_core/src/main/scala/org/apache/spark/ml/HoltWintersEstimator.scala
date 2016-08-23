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
import org.apache.spark.sql.types.StructType


/**
  * Created by dirceu on 19/05/16.
  */
class HoltWintersBestModel[T, M <: TimeSeriesModel](override val uid: String,
                                                    val bestPrediction: RDD[(T, M)]
                                                    , val validationMetrics: RDD[(T, ModelParamEvaluation[T])])
  extends Model[HoltWintersBestModel[T, M]] with TimeSeriesBestModelFinderParam[T] {

  //TODO look for this method usage to see if it can be removed
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): HoltWintersBestModel[T, M] = {
    val copied = new HoltWintersBestModel[T, M](
      uid,
      bestPrediction,
      validationMetrics
    )
    copyValues(copied, extra)
  }
}