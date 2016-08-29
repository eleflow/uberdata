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

import eleflow.uberdata.core.data.DataTransformer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.Row

/**
  * Created by dirceu on 05/07/16.
  */
abstract class BaseTimeSeriesGenerator
    extends Transformer
    with HasInputCol
    with HasOutputCol
    with HasTimeCol
    with DefaultParamsWritable
    with HasLabelCol
    with HasFeaturesCol {

  def convertRowToFloat(toBeConverted: Row): Row = {
    val values = (0 until toBeConverted.length).map { index =>
      val value = toBeConverted.get(index)
      DataTransformer.toFloat(value)
    }
    Row(values)
  }

  def convertRowToDouble(toBeConverted: Row): Row = {
    val values = (0 until toBeConverted.length).map { index =>
      val value = toBeConverted.get(index)
      DataTransformer.toDouble(value)
    }
    Row(values: _*)
  }

  def convertColumnToDouble(toBeTransformed: Row, colIndex: Broadcast[Int]): Row = {
    val (prior, after) = toBeTransformed.toSeq.splitAt(colIndex.value)
    val converted =
      DataTransformer.toDouble(toBeTransformed.get(colIndex.value))
    val result = (prior :+ converted.toDouble) ++ after.tail
    Row(result: _*)
  }
}
