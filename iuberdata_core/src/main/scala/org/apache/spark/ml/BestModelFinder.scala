package org.apache.spark.ml

import com.cloudera.sparkts.models.UberArimaModel
import eleflow.uberdata.enums.SupportedAlgorithm
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 31/05/16.
  */
abstract class BestModelFinder[T, M <: ForecastBaseModel[M]](implicit kt: ClassTag[T], ord: Ordering[T] = null) extends Estimator[M]
  with PredictorParams with HasTimeSeriesEvaluator[T] with HasEstimatorParams
  with HasNFutures with HasValidationCol {

  lazy val partialValidationCol = s"partial${$(validationCol)}"
  lazy val inputOutputDataType = new VectorUDT

  def setValidationCol(value: String) = set(validationCol, value)

  def setLabelCol(label: String) = set(labelCol, label)

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[T]) = set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  protected def train(dataSet: DataFrame): M

  override def fit(dataSet: DataFrame): M = {
    copyValues(train(dataSet).setParent(this)
     )
  }

  protected def split(dataSet: DataFrame, nFutures: Int) = {
    dataSet.foreach { row =>
      val features = row.getAs[org.apache.spark.mllib.linalg.Vector]($(featuresCol))
      if (features.size - nFutures <= 0) throw new IllegalArgumentException(s"Row ${row.toSeq.mkString(",")} has less timeseries attributes than nFutures")
    }
    val data = dataSet.map {
      row =>
        val featuresIndex = row.fieldIndex($(featuresCol))
        val features = row.getAs[org.apache.spark.mllib.linalg.Vector](featuresIndex)
        val trainSize = features.size - nFutures
        val (validationFeatures,toBeValidated) = features.toArray.splitAt(trainSize)
        val validationRow = row.toSeq.updated(featuresIndex, Vectors.dense(validationFeatures)) :+ Vectors.dense(toBeValidated)
        Row(validationRow: _*)
    }
    val context = dataSet.sqlContext
    context.createDataFrame(data, dataSet.schema.add(
      new StructField(partialValidationCol, new VectorUDT)
    )).cache
  }

  def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), inputOutputDataType)
    schema
  }

  override def copy(extra: ParamMap): Estimator[M] = {
    val that = this.getClass.getConstructor(classOf[String], classOf[scala.reflect.ClassTag[T]],
      classOf[scala.math.Ordering[T]]).
      newInstance(uid, kt, ord).setValidationCol($(validationCol))
    copyValues(that, extra)
  }

  protected def arimaEvaluation(row: Row, model: UberArimaModel, broadcastEvaluator: Broadcast[TimeSeriesEvaluator[T]], id: T,
                                parameters: ParamMap): (UberArimaModel, ModelParamEvaluation[T]) = {
    val features = row.getAs[org.apache.spark.mllib.linalg.Vector]($(featuresCol))
    log.warn(s"Evaluating forecast for id $id, with parameters p ${model.p}, d ${model.d} and q ${model.q}")

    val (forecastToBeValidated, _) = model.forecast(features, $(nFutures)).toArray.splitAt(features.size)
    val toBeValidated = features.toArray.zip(forecastToBeValidated)
    val metric = broadcastEvaluator.value.evaluate(toBeValidated)
    val metricName = broadcastEvaluator.value.getMetricName
    (model, new ModelParamEvaluation[T](id, metric, parameters, Some(metricName), SupportedAlgorithm.Arima))
  }
}
