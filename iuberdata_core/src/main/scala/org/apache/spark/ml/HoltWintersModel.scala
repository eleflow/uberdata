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

import com.cloudera.sparkts.models.UberHoltWintersModel
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasGroupByCol, HasNFutures, HasValidationCol}
import org.apache.spark.ml.util.{DefaultParamsReader, _}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
  * Created by dirceu on 17/05/16.
  */
class HoltWintersModel[T](
  override val uid: String,
  val models: RDD[(T, (UberHoltWintersModel, ModelParamEvaluation[T]))]
)(implicit kt: ClassTag[T], ord: Ordering[T] = null)
    extends ForecastBaseModel[HoltWintersModel[T]]
    with HoltWintersParams
    with HasValidationCol
    with HasNFutures
    with HasGroupByCol
    with MLWritable
    with ForecastPipelineStage {

  override def write: MLWriter =
    new HoltWintersModel.HOLTWintersRegressionModelWriter(this)

  override def transform(dataSet: Dataset[_]) = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema)

    val scContext = dataSet.sqlContext.sparkContext
    //TODO fazer com que os modelos invalidos voltem numeros absurdos

    val joined = models.join(dataSet.rdd.map{case (r: Row) => (r.getAs[T]($(groupByCol).get), r)})

    val featuresColName = scContext.broadcast($(featuresCol))
    val nFut = scContext.broadcast($(nFutures))
    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row.getAs[org.apache.spark.ml.linalg.Vector](featuresColName.value)

        val forecast = Vectors.dense(new Array[Double](nFut.value))
        bestModel.forecast(org.apache.spark.mllib.linalg.Vectors.fromML(features), forecast)
        Row(
          row.toSeq :+ forecast.asML :+ SupportedAlgorithm.HoltWinters.toString :+ bestModel.params: _*
        )
    }

    dataSet.sqlContext.createDataFrame(predictions, predSchema)
  }

  lazy val forecast: (T, (UberHoltWintersModel, Row)) => Row = {
    case (id, (model, row)) =>
      val features = row.getAs[Vector]($(featuresCol))
      val futures = $(nFutures)
      val forecast = model.forecast(features, Vectors.dense(Array[Double](futures))).toArray //.drop(features.size)
      val (featuresPrediction, forecastPrediction) =
        forecast.splitAt(features.size)
      Row(
        row.toSeq :+ org.apache.spark.ml.linalg.Vectors.dense(forecastPrediction) :+ org.apache.spark.ml.linalg.Vectors.dense(featuresPrediction): _*
      )
  }

  override def copy(extra: ParamMap): HoltWintersModel[T] = {
    val newModel = copyValues(new HoltWintersModel[T](uid, models), extra)
    newModel.setValidationCol($(validationCol)).asInstanceOf[HoltWintersModel[T]]
  }

}

object HoltWintersModel extends MLReadable[HoltWintersModel[_]] {
  type T = ClassTag[_]

  //TODO avaliar a necessidade deste metodo
  override def read: MLReader[HoltWintersModel[_]] = null

  private[HoltWintersModel] class HOLTWintersRegressionModelWriter(
    instance: HoltWintersModel[_]
  ) extends MLWriter
    {

    //TODO validar este metodo
    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.models.saveAsObjectFile(dataPath)
    }

  }

  private class HOLTWintersRegressionModelReader[T](implicit kt: ClassTag[T],
                                                    ord: Ordering[T] = null)
      extends MLReader[HoltWintersModel[T]] {

    /** Checked against metadata when loading model */
    private val className = classOf[HoltWintersModel[T]].getName

    override def load(path: String): HoltWintersModel[T] = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val models =
        sc.objectFile[(T, (UberHoltWintersModel, ModelParamEvaluation[T]))](
          dataPath
        )

      val holtWintersModel = new HoltWintersModel[T](metadata.uid, models)

      DefaultParamsReader.getAndSetParams(holtWintersModel, metadata)
      holtWintersModel
    }
  }

}
