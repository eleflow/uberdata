package org.apache.spark.ml

import org.apache.spark.Logging
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression._
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Row}
import com.cloudera.sparkts.models.UberArimaModel
import eleflow.uberdata.enums.SupportedAlgorithm.Algorithm
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.param.shared._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Created by dirceu on 12/04/16.
  * Treina e executa a validacao do melhor modelo, retornando o melhor modelo mas os parametros de avaliação de
  * todos os modelos.
  */
class ArimaBestModelFinder[T](override val uid: String)(implicit kt: ClassTag[T])
  extends BestModelFinder[T,ArimaModel[T]]
    with ArimaParams with DefaultParamsWritable
     with HasNFutures
    with TimeSeriesBestModelFinder with Logging {
  def this()(implicit kt: ClassTag[T]) = this(Identifiable.randomUID("arima"))

  def setTimeSeriesEvaluator(eval: TimeSeriesEvaluator[T]) = set(timeSeriesEvaluator, eval)

  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  def setNFutures(value: Int) = set(nFutures, value)

  override def setValidationCol(value: String) = set(validationCol, value)

  def setFeaturesCol(label: String) = set(featuresCol, label)

  def setLabelCol(label: String) = set(labelCol, label)

  def getOrdering(metricName: String): Ordering[Double] = {
    metricName match {
      case "re" => Ordering.Double.reverse
      case _ => Ordering.Double
    }
  }

  def modelEvaluation(idModels: RDD[(T, Row, Seq[(ParamMap, UberArimaModel)])]):
  RDD[(T, (UberArimaModel, Seq[ModelParamEvaluation[T]]))] = {
    val eval = $(timeSeriesEvaluator)
    val broadcastEvaluator = idModels.context.broadcast(eval)
    val ordering = TimeSeriesEvaluator.ordering(eval.getMetricName)
    idModels.map {
      case (id, row, models) =>
        val evaluatedModels = models.map {
          case (parameters, model) =>
            arimaEvaluation(row,model,broadcastEvaluator,id,parameters)
        }
        val sorted = evaluatedModels.sortBy(_._2.metricResult)(ordering)
        log.warn(s"best model reach ${sorted.head._2.metricResult}")
        log.warn(s"best model params ${sorted.head._2.params}")

        val (bestModel, _) = sorted.head
        (id, (bestModel.asInstanceOf[UberArimaModel], sorted.map(_._2)))
    }
  }

  override protected def train(dataSet: DataFrame): ArimaModel[T] = {
    val splitDs = split(dataSet, $(nFutures))
    val idModels = splitDs.rdd.map(train)
    new ArimaModel[T](uid, modelEvaluation(idModels)).setValidationCol($(validationCol)).asInstanceOf[ArimaModel[T]]
  }

  def train(row: Row): (T, Row, Seq[(ParamMap, UberArimaModel)]) = {
    val id = row.getAs[T]($(labelCol))
    val result =  $(estimatorParamMaps).flatMap {
        params =>

          val q = params.getOrElse(arimaQ, 0)
          val p = params.getOrElse(arimaP, 0)
          val d = params.getOrElse(arimaD, 0)
           try {
            Some((params, UberArimaModel.fitModel(p, d, q, row.getAs($(featuresCol)))))
          } catch {
            case e: Exception =>
              log.error(s"Got the following Exception ${e.getLocalizedMessage} when using params P $p, Q$q and D$d " +
                s"in id $id")
              None
          }
      }.toSeq
    (id, row,result)
  }
}
case class ModelParamEvaluation[T](id: T, metricResult: Double, params: ParamMap, metricName: Option[String] = None, algorithm:Algorithm)

object ArimaBestModelFinder extends DefaultParamsReadable[ArimaBestModelFinder[_]] {

  override def load(path: String): ArimaBestModelFinder[_] = super.load(path)
}


class ArimaTrainingSummary[T](predictions: DataFrame,
                              predictionCol: String,
                              labelCol: String,
                              model: ArimaModel[T],
                              diagInvAtWA: Array[Double],
                              val featuresCol: String,
                              val objectiveHistory: Array[Double])
  extends ARIMALinearSummary[T](predictions, predictionCol, labelCol, model, diagInvAtWA)
