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
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created by dirceu on 13/07/16.
  */
class VectorizeEncoder(override val uid: String)
    extends Transformer
    with HasIdCol
    with HasInputCols
    with HasLabelCol
    with HasGroupByCol
    with HasOutputCol
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("vectorizer"))

  def setIdCol(input: String) = set(idCol, input)

  def setLabelCol(input: String) = set(labelCol, input)

  def setGroupByCol(toGroupBy: String) = set(groupByCol, toGroupBy)

  def setInputCol(input: Array[String]) = set(inputCols, input)

  def setOutputCol(output: String) = set(outputCol, output)

  override def transform(dataSet: DataFrame): DataFrame = {
    val context = dataSet.sqlContext.sparkContext
    val input = context.broadcast($(inputCols))
    val allColumnNames = dataSet.schema.map(_.name)
    val nonInputColumnIndexes = context.broadcast(
      allColumnNames.zipWithIndex.filter(
        f => !$(inputCols).contains(f._1) || f._1 == $(groupByCol) || f._1 == $(idCol)
      )
    )
    val result = dataSet.map { row =>
      val rowSeq = row.toSeq
      val nonInputColumns = nonInputColumnIndexes.value.map {
        case (_, index) => rowSeq(index)
      }
      val size = input.value.length
      val (values, indices) = input.value
        .filter(col => row.getAs(col) != null)
        .map { column =>
          DataTransformer.toDouble(row.getAs(column))
        }
        .zipWithIndex
        .filter(f => f._1 != 0d)
        .unzip
      Row(
        nonInputColumns :+ org.apache.spark.mllib.linalg.Vectors
          .sparse(size, indices.toArray, values.toArray): _*
      )
    }
    val newSchema = transformSchema(dataSet.schema)
    dataSet.sqlContext.createDataFrame(result, newSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =
    StructType(
      schema.filter(
        col =>
          !$(inputCols).contains(col.name) || col.name == $(groupByCol) || col.name == $(
            idCol
          )
            || col.name == $(labelCol)
      )
    ).add(StructField($(outputCol), new VectorUDT))
}
