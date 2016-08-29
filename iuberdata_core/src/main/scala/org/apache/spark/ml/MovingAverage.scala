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


import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.param.shared.{
  HasInputCol,
  HasOutputCol
}
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
  * Created by celio on 29/04/16.
  */
class MovingAverage[T](override val uid: String)
    extends Transformer
    with HasInputCol
    with HasOutputCol
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("movingAverage"))

  /**
    * the window size of the moving average
    * Default: [[3]]
    *
    * @group param
    */
  val windowSize: IntParam = new IntParam(this, "windowSize", "window size")

  /** @group setParam */
  def setWindowSize(value: Int): this.type = set(windowSize, value)

  /** @group getParam */
  def getWindowSize: Int = $(windowSize)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  setDefault(windowSize -> 3)

  override def transform(dataSet: DataFrame): DataFrame = {
    val outputSchema = transformSchema(dataSet.schema)
    val sparkContext = dataSet.sqlContext.sparkContext
    val inputType = outputSchema($(inputCol)).dataType
    val inputTypeBr = sparkContext.broadcast(inputType)
    val dataSetRdd = dataSet.rdd
    val inputColName = sparkContext.broadcast($(inputCol))
    val inputColIndex = dataSet.columns.indexOf($(inputCol))
    val inputColIndexBr = sparkContext.broadcast(inputColIndex)
    val windowSizeBr = sparkContext.broadcast($(windowSize))
    val maRdd = dataSetRdd.map { row =>
      val (array, rawValue) = if (inputTypeBr.value.isInstanceOf[VectorUDT]) {
        val vector =
          row.getAs[org.apache.spark.mllib.linalg.Vector](inputColName.value)
        (vector.toArray, Vectors.dense(vector.toArray.drop(windowSizeBr.value - 1)))
      } else {
        val iterable = row.getAs[Iterable[Double]](inputColName.value)
        (iterable.toArray, Vectors.dense(iterable.toArray.drop(windowSizeBr.value - 1)))
      }
      val (before, after) = row.toSeq.splitAt(inputColIndexBr.value)
      Row(
        (before :+ rawValue) ++ after.tail :+ MovingAverageCalc
          .simpleMovingAverageArray(array, windowSizeBr.value): _*
      )
    }
    dataSet.sqlContext.createDataFrame(maRdd, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField($(outputCol), ArrayType(DoubleType)))
  }

  override def copy(extra: ParamMap): MovingAverage[T] = defaultCopy(extra)
}

object MovingAverageCalc {
  private[ml] def simpleMovingAverageArray(values: Array[Double], period: Int): Array[Double] = {
    (for (i <- 1 to values.length)
      yield
      //TODO rollback this comment with the right size of features to make the meanaverage return
      // the features values for the first values of the calc
      if (i < period) 0d //values(i)
      else values.slice(i - period, i).sum / period).toArray.dropWhile(_ == 0d)
  }
}

object MovingAverage extends DefaultParamsReadable[MovingAverage[_]] {

  override def load(path: String): MovingAverage[_] = super.load(path)
}
