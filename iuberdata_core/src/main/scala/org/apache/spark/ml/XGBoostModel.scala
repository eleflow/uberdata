package org.apache.spark.ml

import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.dmlc.xgboost4j.{LabeledPoint => XGBLabeledPoint}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.XGBoostModel.XGBoostRegressionModelWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasGroupByCol, HasIdCol, HasLabelCol}
import org.apache.spark.ml.util.{DefaultParamsReader, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{Logging, ml}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 30/06/16.
  */
class XGBoostModel[I](override val uid: String,
                      val models: RDD[(I, (UberXGBOOSTModel, Seq[(ModelParamEvaluation[I])]))])(implicit kt: ClassTag[I], ord: Ordering[I] = null)
  extends ForecastBaseModel[XGBoostModel[I]]
    with HasGroupByCol
    with HasIdCol
    with HasFeaturesCol
    with HasLabelCol
    with MLWritable with ForecastPipelineStage {

  private var trainingSummary: Option[XGBoostTrainingSummary[I]] = None

  def setGroupByCol(value: String) = set(groupByCol, value)

  def setIdCol(value: String) = set(idCol, value)

  def setLabelCol(value: String) = set(labelCol, value)

  def setSummary(summary: XGBoostTrainingSummary[I]) = {
    trainingSummary = Some(summary)
    this
  }

  override def write: MLWriter = new XGBoostRegressionModelWriter(this)

  override def transform(dataSet: DataFrame) = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema)

    val joined = models.join(dataSet.map(r => (r.getAs[I]($(groupByCol)), r)))

    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row.getAs[org.apache.spark.mllib.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
        val idColumnIndex = row.fieldIndex($(idCol))
        val featuresIndex = row.fieldIndex(IUberdataForecastUtil.FEATURES_COL_NAME)
        val groupByColumnIndex = row.fieldIndex($(groupByCol))
        val rowValues = row.toSeq.zipWithIndex.filter { case (_, index) => index == idColumnIndex || index == featuresIndex ||
          index == groupByColumnIndex
        }.map(_._1)
        val featuresAsFloat = features.toArray.map(_.toFloat)
        //val labeledPoints = Iterator(ml.dmlc.xgboost4j.LabeledPoint.fromDenseVector(featuresAsFloat.head,
        //  featuresAsFloat.tail))
        val labeledPoints = Iterator(XGBLabeledPoint.fromDenseVector(0, featuresAsFloat))
        val forecast = bestModel.boosterInstance.predict(new DMatrix(labeledPoints, null))
          .flatMap(_.map(_.toDouble))
        /*Row(groupByColumnIndex, features, SupportedAlgorithm.XGBoostAlgorithm.toString ,
          bestModel.params.map(f => f._1 -> f._2.toString)
          , forecast.head)*/
        Row(rowValues :+ SupportedAlgorithm.XGBoostAlgorithm.toString :+
          bestModel.params.map(f => f._1 -> f._2.toString) :+ forecast.head: _*)
    }
    dataSet.sqlContext.createDataFrame(predictions, predSchema).cache
  }

  def convertLabeledPoint(lb: org.apache.spark.mllib.regression.LabeledPoint) = {
    XGBLabeledPoint.fromDenseVector(lb.label.toFloat, lb.features.toArray.map(_.toFloat))
  }

  def convertNullValues: Row => Row = {
    a: Row => Row(a.toSeq.map(f => if (f == null) 0l else f): _*)
  }

  /*override def transformSchema(schema: StructType) = StructType(super.transformSchema(schema).filter(f =>
    Seq(IUberdataForecastUtil.FEATURES_COL_NAME, $(featuresCol), $(groupByCol),IUberdataForecastUtil.ALGORITHM,
      IUberdataForecastUtil.PARAMS).contains(f.name))).add(StructField(IUberdataForecastUtil.FEATURES_PREDICTION_COL_NAME,
    DoubleType))*/

  override def transformSchema(schema: StructType) = StructType(super.transformSchema(schema).filter(f =>
    Seq($(idCol), IUberdataForecastUtil.FEATURES_COL_NAME, $(featuresCol), $(groupByCol), IUberdataForecastUtil.ALGORITHM,
      IUberdataForecastUtil.PARAMS).contains(f.name))).add(StructField(IUberdataForecastUtil.FEATURES_PREDICTION_COL_NAME,
    DoubleType))


  override def copy(extra: ParamMap): XGBoostModel[I] = {
    val newModel = copyValues(new XGBoostModel[I](uid, models), extra)
    trainingSummary.map(summary => newModel.setSummary(summary))
    newModel
      .setGroupByCol($(groupByCol))
      .setIdCol($(idCol))
      .setLabelCol($(labelCol))
      .setValidationCol($(validationCol))
      .asInstanceOf[XGBoostModel[I]]
  }
}

object XGBoostModel extends MLReadable[XGBoostModel[_]] {
  type T = ClassTag[_]

  override def read: MLReader[XGBoostModel[_]] = null

  private[XGBoostModel] class XGBoostRegressionModelWriter(instance: XGBoostModel[_])
    extends MLWriter with Logging {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.models.saveAsObjectFile(dataPath)
    }
  }

  private class XGBoostRegressionModelReader[T](implicit kt: ClassTag[T], ord: Ordering[T] = null) extends MLReader[XGBoostModel[T]] {

    /** Checked against metadata when loading model */
    private val className = classOf[XGBoostModel[T]].getName

    override def load(path: String): XGBoostModel[T] = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val models = sc.objectFile[(T, (UberXGBOOSTModel, Seq[ModelParamEvaluation[T]]))](dataPath)

      val arimaModel = new XGBoostModel[T](metadata.uid, models)

      DefaultParamsReader.getAndSetParams(arimaModel, metadata)
      arimaModel
    }
  }

}