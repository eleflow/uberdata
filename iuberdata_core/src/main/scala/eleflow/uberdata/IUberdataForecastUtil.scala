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

import eleflow.uberdata.core.IUberdataContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit

/**
  * Created by dirceu on 31/05/16.
  */
object IUberdataForecastUtil {

  lazy val FEATURES_PREDICTION_COL_NAME = "featuresPrediction"
  lazy val FEATURES_COL_NAME = "features"
  lazy val ALGORITHM = "algorithm"
  lazy val PARAMS = "parameters"
  lazy val METRIC_COL_NAME = "metric"

  def convertColumnToLong(row: Row, columnIndex: Int): Row = {
    row.get(columnIndex) match {
      case s: java.sql.Timestamp =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ s.getTime) ++ after.tail :+ s
        Row(result: _*)
      case d: Double =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ d.toLong) ++ after.tail :+ d
        Row(result: _*)
      case i: Int =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ i.toLong) ++ after.tail :+ i
        Row(result: _*)
      case s: Short =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ s.toLong) ++ after.tail :+ s
        Row(result: _*)
      case _ => row
    }
  }

  def convertColumnToLongAddAtEnd(row: Row, columnIndex: Int): Row = {
    val result = row.get(columnIndex) match {
      case s: java.sql.Timestamp =>
        val result = row.toSeq :+ s.getTime
        Row(result: _*)
      case d: Double =>
        val result = row.toSeq :+ d.toLong
        Row(result: _*)
      case i: Int =>
        val result = row.toSeq :+ i.toLong
        Row(result: _*)
      case s: Short =>
        val result = row.toSeq :+ s.toLong
        Row(result: _*)
      case _ => row
    }
    result
  }

  def createIdColColumn(dataFrame : DataFrame, context : IUberdataContext) : DataFrame = {
    val arrId = dataFrame.rdd.zipWithIndex.map(
      x => x._1.toSeq :+ x._2
    ).map(
      x => Row.fromSeq(x))
    context.sqlContext.createDataFrame(arrId,
      dataFrame.withColumn("idCol", lit(1L : Long)).schema)
  }

}
