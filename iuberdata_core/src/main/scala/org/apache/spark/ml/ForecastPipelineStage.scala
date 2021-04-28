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
import org.apache.spark.ml.param.shared.{HasNFutures, HasPredictionCol, HasValidationCol}
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types.{StructType, StringType, StructField, MapType}

/**
  * Created by dirceu on 01/06/16.
  */
trait ForecastPipelineStage
    extends PipelineStage
    with HasNFutures
    with HasPredictionCol
    with HasValidationCol {

  def setValidationCol(value: String): this.type = set(validationCol, value)

  override def transformSchema(schema: StructType): StructType = {
    schema
      .add(StructField($(validationCol), new VectorUDT))
      .add(StructField(IUberdataForecastUtil.ALGORITHM, StringType))
      .add(StructField(IUberdataForecastUtil.PARAMS, MapType(StringType, StringType)))
  }
}
