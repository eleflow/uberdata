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

import com.cloudera.sparkts.models.UberArimaModel
import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasNFutures
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 12/04/16.
  * Conter o melhor modelo para cada loja assim como os parametros de avaliaçãod e todos os modelos
  */
class ArimaModel[G](
  override val uid: String,
  val models: RDD[(G, (UberArimaModel, Seq[(ModelParamEvaluation[G])]))]
)(implicit kt: ClassTag[G], ord: Ordering[G] = null)
    extends ForecastBaseModel[ArimaModel[G]]
    with ArimaParams
    with HasNFutures
    with MLWritable
    with ForecastPipelineStage {

  private var trainingSummary: Option[ArimaTrainingSummary[G]] = None

  def setSummary(summary: ArimaTrainingSummary[G]) = {
    trainingSummary = Some(summary)
    this
  }

  override def write: MLWriter =
    new ArimaModel.ARIMARegressionModelWriter(this)

  override def transform(dataSet: DataFrame) = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema)
    val scContext = dataSet.sqlContext.sparkContext
    //TODO fazer com que os modelos invalidos voltem numeros absurdos

    val joined = models.join(dataSet.map(r => (r.getAs[G]($(groupByCol).get), r)))

    val featuresColName =
      dataSet.sqlContext.sparkContext.broadcast($(featuresCol))
    val nFut = scContext.broadcast($(nFutures))
    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row.getAs[org.apache.spark.mllib.linalg.Vector](featuresColName.value)
        val (ownFeaturesPrediction, forecast) =
          bestModel.forecast(features, nFut.value).toArray.splitAt(features.size)
        Row(
          row.toSeq :+ Vectors
            .dense(forecast) :+ SupportedAlgorithm.Arima.toString :+ bestModel.params :+ Vectors
            .dense(ownFeaturesPrediction): _*
        )
    }
    dataSet.sqlContext.createDataFrame(predictions, predSchema)
  }

  override def transformSchema(schema: StructType) =
    super
      .transformSchema(schema)
      .add(
        StructField(
          IUberdataForecastUtil.FEATURES_PREDICTION_COL_NAME,
          new VectorUDT
        )
      )

  override def copy(extra: ParamMap): ArimaModel[G] = {
    val newModel = copyValues(new ArimaModel[G](uid, models), extra)
    trainingSummary.map(summary => newModel.setSummary(summary))
    newModel.setValidationCol($(validationCol)).asInstanceOf[ArimaModel[G]]
  }

}

object ArimaModel extends MLReadable[ArimaModel[_]] {
  type T = ClassTag[_]

  //TODO avaliar a necessidade deste metodo
  override def read: MLReader[ArimaModel[_]] = null

  private[ArimaModel] class ARIMARegressionModelWriter(instance: ArimaModel[_])
      extends MLWriter
      with Logging {

    //TODO validar este metodo
    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.models.saveAsObjectFile(dataPath)
    }

  }

  private class ARIMARegressionModelReader[T](implicit kt: ClassTag[T], ord: Ordering[T] = null)
      extends MLReader[ArimaModel[T]] {

    /** Checked against metadata when loading model */
    private val className = classOf[ArimaModel[T]].getName

    override def load(path: String): ArimaModel[T] = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val models =
        sc.objectFile[(T, (UberArimaModel, Seq[ModelParamEvaluation[T]]))](
          dataPath
        )

      val arimaModel = new ArimaModel[T](metadata.uid, models)

      DefaultParamsReader.getAndSetParams(arimaModel, metadata)
      arimaModel
    }
  }

}
