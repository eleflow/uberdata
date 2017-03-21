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

package eleflow.uberdata.data

import org.apache.spark.ml.{MovingAverage, TimeSeriesGenerator}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.scalatest.mock.EasyMockSugar
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by celio on 04/05/16.
  */
class TestPipelineTransformer
    extends FlatSpec
    with Matchers
    with EasyMockSugar
    with BeforeAndAfterWithContext {

  "Moving Average Transformer" should "return the moving averages of a time series " in {

    val testData = context.sqlContext
      .createDataFrame(
        Seq(
          (1.0, Vectors.dense(1.0, 3.0, 5.0)),
          (2.0, Vectors.dense(3.0, 5.0, 7.0)),
          (3.0, Vectors.dense(5.0, 7.0, 9.0))
        ))
      .toDF("id", "timeseries")

    val ma = new MovingAverage[Double]()
    ma.setInputCol("timeseries")
    ma.setOutputCol("MovingAverages")

    val transformedData = ma.transform(testData)
    val namesArray = transformedData.columns
    val idsArray = transformedData.rdd.map(f => f.get(0)).collect
    val timeSeriesArray =
      transformedData.rdd.map(f => f.get(1)).collect.map(_.asInstanceOf[DenseVector].toArray)
    val maArray = transformedData.rdd.map(f => f.get(2)).collect

    assert(namesArray.length == 3)
    assert(namesArray(0) == "id")
    assert(namesArray(1) == "timeseries")
    assert(namesArray(2) == "MovingAverages")
    assert(Array(1.0, 2.0, 3.0).deep == idsArray.deep)
    assert(Array(Array(5.0), Array(7.0), Array(9.0)).deep == timeSeriesArray.deep)
    assert(Array(Array(3.0), Array(5.0), Array(7.0)).deep == maArray.deep)
  }

  it should "return the moving averages of a time series considering a change in the window size parameter" in {

    val testData = context.sqlContext
      .createDataFrame(
        Seq(
          (1.0, Vectors.dense(1.0, 3.0, 5.0), 1d),
          (2.0, Vectors.dense(3.0, 5.0, 7.0), 2d),
          (3.0, Vectors.dense(5.0, 7.0, 9.0), 3d)
        ))
      .toDF("id", "timeseries", "date")

    val ma = new MovingAverage[Double]()
    ma.setInputCol("timeseries")
    ma.setOutputCol("MovingAverages")
    ma.setWindowSize(2)
    val transformedData = ma.transform(testData)
    val namesArray = transformedData.columns
    val idsArray = transformedData.rdd.map(f => f.get(0)).collect
    val timeSeriesArray =
      transformedData.rdd.map(f => f.get(1)).collect.map(_.asInstanceOf[DenseVector].toArray)
    val maArray = transformedData.rdd.map(f => f.get(3)).collect

    assert(ma.getWindowSize == 2)
    assert(namesArray.length == 4)
    assert(namesArray(0) == "id")
    assert(namesArray(1) == "timeseries")
    assert(namesArray(2) == "date")
    assert(namesArray(3) == "MovingAverages")
    assert(Array(1.0, 2.0, 3.0).deep == idsArray.deep)
    assert(Array(Array(3.0, 5.0), Array(5.0, 7.0), Array(7.0, 9.0)).deep == timeSeriesArray.deep)
    assert(Array(Array(2.0, 4.0), Array(4.0, 6.0), Array(6.0, 8.0)).deep == maArray.deep)

  }

  it should "return the last values" in {

    val testData = context.sqlContext
      .createDataFrame(
        Seq(
          (1.0, Vectors.dense(1.0, 3.0, 5.0, 7.0), 1d),
          (2.0, Vectors.dense(3.0, 5.0, 7.0, 9.0), 2d),
          (3.0, Vectors.dense(5.0, 7.0, 9.0, 11.0), 3d)
        ))
      .toDF("id", "timeseries", "date")

    val ma = new MovingAverage[Double]()
    ma.setInputCol("timeseries")
    ma.setOutputCol("MovingAverages")
    ma.setWindowSize(2)

    val transformedData = ma.transform(testData)
    val labelArray = transformedData.rdd.map(f => f.getAs[Double]("id")).collect
    val timeSeriesArray = transformedData.rdd
      .map(f => f.getAs[org.apache.spark.mllib.linalg.Vector]("timeseries"))
      .collect
      .map {
        _.toArray.mkString("[", ",", "]")
      }
    //return more than specified to avoid 0 values in moving average algorithm
    val correctTimeSeriesArray = Array("[3.0,5.0,7.0]", "[5.0,7.0,9.0]", "[7.0,9.0,11.0]")

    assert(Array(1.0, 2.0, 3.0).deep == labelArray.deep)
    assert(correctTimeSeriesArray.deep == timeSeriesArray.deep)
  }

  "Time Series Generator Transformer" should "return a time series " in {

    val testData = context.sqlContext
      .createDataFrame(
        Seq(
          (1.0, 1.0, 2d),
          (2.0, 2.0, 3d),
          (3.0, 1.0, 4d),
          (4.0, 2.0, 5d),
          (5.0, 1.0, 6d),
          (6.0, 2.0, 7d),
          (7.0, 1.0, 8d),
          (8.0, 2.0, 9d),
          (9.0, 1.0, 10d),
          (10.0, 2.0, 11d)
        ))
      .toDF("sales", "store", "date")

    val tsg = new TimeSeriesGenerator[Double]()
      .setOutputCol("features")
      .setGroupByCol("store")
      .setFeaturesCol("sales")
      .setTimeCol("date")
    val transformedData = tsg.transform(testData)
    val namesArray = transformedData.columns
    val labelArray = transformedData.rdd.map(f => f.get(0)).collect
    val timeSeriesArray = transformedData.rdd.map(f => f.get(1)).collect.map {
      _.toString
    }
    val correctTimeSeriesArray = Array("[2.0,4.0,6.0,8.0,10.0]", "[1.0,3.0,5.0,7.0,9.0]")

    assert(namesArray.length == 2)
    assert(namesArray(0) == "store")
    assert(namesArray(1) == "features")
    assert(Array(2.0, 1.0).deep == labelArray.deep)
    assert(correctTimeSeriesArray.deep == timeSeriesArray.deep)
  }

  it should "return time series considering a change in parameters" in {

    val testData = context.sqlContext
      .createDataFrame(
        Seq(
          (1.0, 1.0, 1d),
          (2.0, 2.0, 2d),
          (1.0, 3.0, 3d),
          (2.0, 4.0, 4d),
          (1.0, 5.0, 5d),
          (2.0, 6.0, 6d),
          (1.0, 7.0, 7d),
          (2.0, 8.0, 8d),
          (1.0, 9.0, 9d),
          (2.0, 10.0, 10d)
        ))
      .toDF("store", "sales", "date")

    val tsg = new TimeSeriesGenerator[Double]().setOutputCol("features")
    tsg.setGroupByCol("store")
    tsg.setFeaturesCol("sales")
    tsg.setTimeCol("date")
    val transformedData = tsg.transform(testData)
    val namesArray = transformedData.columns
    val labelArray = transformedData.rdd.map(f => f.get(0)).collect
    val timeSeriesArray = transformedData.rdd.map(f => f.get(1)).collect.map {
      _.toString
    }
    val correctTimeSeriesArray = Array("[2.0,4.0,6.0,8.0,10.0]", "[1.0,3.0,5.0,7.0,9.0]")

    assert(namesArray.length == 2)
    assert(namesArray(0) == "store")
    assert(namesArray(1) == "features")
    assert(Array(2.0, 1.0).deep == labelArray.deep)
    assert(correctTimeSeriesArray.deep == timeSeriesArray.deep)

  }

  it should "sort the data by timeseries" in {

    val testData = context.sqlContext
      .createDataFrame(
        Seq(
          (1.0, 1.0, 2d),
          (2.0, 2.0, 3d),
          (3.0, 1.0, 7d),
          (4.0, 2.0, 5d),
          (5.0, 1.0, 4d),
          (6.0, 2.0, 6d),
          (7.0, 1.0, 8d),
          (8.0, 2.0, 13d),
          (9.0, 1.0, 10d),
          (10.0, 2.0, 11d)
        ))
      .toDF("sales", "store", "date")

    val tsg = new TimeSeriesGenerator[Double]().setOutputCol("features")
    tsg.setGroupByCol("store")
    tsg.setFeaturesCol("sales")
    tsg.setTimeCol("date")

    val transformedData = tsg.transform(testData)
    val namesArray = transformedData.columns
    val labelArray = transformedData.rdd.map(f => f.get(0)).collect
    val timeSeriesArray = transformedData.rdd.map(f => f.get(1)).collect.map {
      _.toString
    }
    val correctTimeSeriesArray = Array("[2.0,4.0,6.0,10.0,8.0]", "[1.0,5.0,3.0,7.0,9.0]")

    assert(namesArray.length == 2)
    assert(namesArray(0) == "store")
    assert(namesArray(1) == "features")
    assert(Array(2.0, 1.0).deep == labelArray.deep)
    assert(correctTimeSeriesArray.deep == timeSeriesArray.deep)
  }
}
