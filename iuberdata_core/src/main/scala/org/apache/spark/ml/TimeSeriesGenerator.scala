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
import org.apache.spark.annotation.Since

import org.apache.spark.ml.param.ParamMap

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.ClassTag

/**
  * Created by celio on 05/05/16.
  */
class TimeSeriesGenerator[L](
  override val uid: String
)(implicit ct: ClassTag[L])
    extends BaseTimeSeriesGenerator {

  def this()(implicit ct: ClassTag[L]) =
    this(Identifiable.randomUID("TimeSeriesGenerator"))

  def setLabelCol(value: String) = set(labelCol, value)

  def setTimeCol(colName: String) = set(timeCol, colName)

  def setFeaturesCol(value: String) = set(featuresCol, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataSet: DataFrame): DataFrame = {
    val rdd = dataSet.rdd

    val sparkContext = dataSet.sqlContext.sparkContext
    val index = sparkContext.broadcast(dataSet.schema.fieldIndex($(timeCol)))
    val labelColIndex =
      sparkContext.broadcast(dataSet.schema.fieldIndex($(labelCol)))
    val featuresColIndex =
      sparkContext.broadcast(dataSet.schema.fieldIndex($(featuresCol)))
    val grouped = rdd.map { row =>
      val timeColRow =
        IUberdataForecastUtil.convertColumnToLong(row, index.value)
      convertColumnToDouble(timeColRow, featuresColIndex)
    }.groupBy { row =>
      row.getAs[L](labelColIndex.value)
    }.map {
      case (key, values) =>
        val toBeUsed =
          values.toArray.sortBy(row => row.getAs[Long](index.value))
        (key, toBeUsed)
    }

    val toBeTrained = grouped.map {
      case (key, values) =>
        org.apache.spark.sql.Row(
          key,
          Vectors.dense(values.map(_.getAs[Double](featuresColIndex.value)))
        )
    }

    val trainSchema = transformSchema(dataSet.schema)
    dataSet.sqlContext.createDataFrame(toBeTrained, trainSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    val labelIndex = schema.fieldIndex($(labelCol))
    StructType(
      Seq(
        schema.fields(labelIndex),
        StructField($(outputCol), new org.apache.spark.mllib.linalg.VectorUDT)
      )
    )
  }

  override def copy(extra: ParamMap): TimeSeriesGenerator[L] =
    defaultCopy(extra)

}

@Since("1.6.0")
object TimeSeriesGenerator extends DefaultParamsReadable[TimeSeriesGenerator[_]] {

  @Since("1.6.0")
  override def load(path: String): TimeSeriesGenerator[_] = super.load(path)
}
