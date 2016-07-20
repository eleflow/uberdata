package org.apache.spark.ml

import com.cloudera.sparkts.models.{HOLTWinters, HOLTWintersModel}
import org.apache.spark.Logging
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.reflect.ClassTag

/**
  * Created by dirceu on 19/05/16.
  */
class HoltWintersBestModelFinder[T](override val uid: String)(implicit kt: ClassTag[T])
  extends HoltWintersBestModelEvaluation[T, HoltWintersModel[T]]
    with DefaultParamsWritable
    with TimeSeriesBestModelFinder
    with Logging {

  import org.apache.spark.sql.DataFrame

  def this()(implicit kt: ClassTag[T]) = this(Identifiable.randomUID("arima"))

  def modelEvaluation(idModels: RDD[(T, Row, Option[HOLTWintersModel])]): RDD[(T, (HOLTWintersModel, ModelParamEvaluation[T]))] = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)
    idModels.filter(_._3.isDefined).map {
      case (id, row, models) =>
        val evaluatedModels = models.map {
          model =>
            holtWintersEvaluation(row, model, broadcastEvaluator, id)
        }.head
        log.warn(s"best model reach ${evaluatedModels._2.metricResult}")
        (id, evaluatedModels)
    }
  }

  override protected def train(dataSet: DataFrame): HoltWintersModel[T] = {
    val splitDs = split(dataSet, $(nFutures))
    val idModels = splitDs.rdd.map(train)
    new HoltWintersModel[T](uid, modelEvaluation(idModels)).setValidationCol($(validationCol)).
      asInstanceOf[HoltWintersModel[T]]
  }

  def train(row: Row): (T, Row, Option[HOLTWintersModel]) = {
    val id = row.getAs[T]($(labelCol))

    val result = try {
      Some(HOLTWinters.fitModel(row.getAs($(featuresCol)), $(nFutures)))
    } catch {
      case e: Exception =>
        log.error(s"Got the following Exception ${e.getLocalizedMessage} in id $id")
        None
    }
    (id, row, result)
  }
}

object HoltWintersBestModelFinder extends DefaultParamsReadable[HoltWintersBestModelFinder[_]] {

  override def load(path: String): HoltWintersBestModelFinder[_] = super.load(path)
}
