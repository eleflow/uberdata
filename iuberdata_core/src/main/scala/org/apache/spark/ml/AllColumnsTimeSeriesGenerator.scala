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

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 04/07/16.
  */
class AllColumnsTimeSeriesGenerator[T, U](
  override val uid: String
)(implicit ct: ClassTag[T])
    extends BaseTimeSeriesGenerator {

  def this()(implicit ct: ClassTag[T]) =
    this(Identifiable.randomUID("AllColumnsTimeSeriesGenerator"))

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setTimeCol(colName: String): this.type = set(timeCol, colName)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataSet: DataFrame): DataFrame = {
    val rdd = dataSet.rdd
    val sparkContext = dataSet.sqlContext.sparkContext
    val labelColIndex =
      sparkContext.broadcast(dataSet.schema.fieldIndex($(labelCol)))
    val keyValueDataSet = rdd.map { row =>
      Row(
        row.getAs[T](labelColIndex.value),
        row.getAs[org.apache.spark.mllib.linalg.Vector]($(featuresCol))
      )
    }
    val trainSchema = transformSchema(dataSet.schema)

    dataSet.sqlContext.createDataFrame(keyValueDataSet, trainSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.filter(_.name == $(labelCol)).head +: Seq(
        StructField($(outputCol), new org.apache.spark.mllib.linalg.VectorUDT)
      )
    )
  }

  override def copy(extra: ParamMap): AllColumnsTimeSeriesGenerator[T, U] =
    defaultCopy(extra)
}


object AllColumnsTimeSeriesGenerator
    extends DefaultParamsReadable[AllColumnsTimeSeriesGenerator[_, _]] {

  override def load(path: String): AllColumnsTimeSeriesGenerator[_, _] =
    super.load(path)
}
