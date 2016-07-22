package org.apache.spark.ml

import eleflow.uberdata.IUberdataForecastUtil
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.ml.XGBoostModel.XGBoostRegressionModelWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasNFutures
import org.apache.spark.ml.util.{DefaultParamsReader, _}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 30/06/16.
  */
class XGBoostModel[T](override val uid: String,
                      val models: RDD[(T, (UberXGBOOSTModel, Seq[(ModelParamEvaluation[T])]))])(implicit kt: ClassTag[T], ord: Ordering[T] = null)
  extends ForecastBaseModel[XGBoostModel[T]]
    with ArimaParams
    with HasNFutures
    with MLWritable with ForecastPipelineStage {

  private var trainingSummary: Option[XGBoostTrainingSummary[T]] = None

  def setSummary(summary: XGBoostTrainingSummary[T]) = {
    trainingSummary = Some(summary)
    this
  }

  override def write: MLWriter = new XGBoostRegressionModelWriter(this)

  override def transform(dataSet: DataFrame) = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema) //TODO mudar pra groupbycol

    //TODO fazer com que os modelos invalidos voltem numeros absurdos
    val joined = models.join(dataSet.map(r => (r.getAs[T]($(featuresCol)), r)))

    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
        val featuresColValue = row.getAs[Double]($(featuresCol))
        val featuresValues = features.toArray.map(_.toFloat)
        val convertedFeatures = Iterator(ml.dmlc.xgboost4j.LabeledPoint.fromDenseVector(featuresValues.head,
          featuresValues.tail))
        val forecast = bestModel.boosterInstance.predict(new DMatrix(convertedFeatures, null)).flatMap(_.map(_.toDouble))
        val row2 = Row(featuresColValue, features, SupportedAlgorithm.XGBoostAlgorithm.toString , bestModel.params.map(f => f._1 -> f._2.toString)
          , forecast.head)
        row2
    }
    dataSet.sqlContext.createDataFrame(predictions, predSchema).cache
  }

//  private def toFloat(label: Any) = {
//    label match {
//      case s: Short => s.toFloat
//      case l: Long => l.toFloat
//      case f: Float => f
//      case d: Double => d.toFloat
//      case s: String => s.toFloat
//      case i: Int => i.toFloat
//    }
//  }

  def tranformLabeledPoint(lb: org.apache.spark.mllib.regression.LabeledPoint) = {
    ml.dmlc.xgboost4j.LabeledPoint.fromDenseVector(lb.label.toFloat, lb.features.toArray.map(_.toFloat))
  }

  def convertNullValues: Row => Row = {
    a: Row => Row(a.toSeq.map(f => if (f == null) 0l else f): _*)
  }

  override def transformSchema(schema: StructType) = StructType(super.transformSchema(schema).filter(f => Seq("features",
    $(labelCol), $(featuresCol),IUberdataForecastUtil.ALGORITHM, IUberdataForecastUtil.PARAMS).contains(f.name)))
    .add(StructField("featuresPrediction", DoubleType))

  override def copy(extra: ParamMap): XGBoostModel[T] = {
    val newModel = copyValues(new XGBoostModel[T](uid, models), extra)
    trainingSummary.map(summary => newModel.setSummary(summary))
    newModel
      .setValidationCol($(validationCol)).asInstanceOf[XGBoostModel[T]]
  }

}

object XGBoostModel extends MLReadable[XGBoostModel[_]] {
  type T = ClassTag[_]

  //TODO avaliar a necessidade deste metodo
  override def read: MLReader[XGBoostModel[_]] = null

  private[XGBoostModel] class XGBoostRegressionModelWriter(instance: XGBoostModel[_])
    extends MLWriter with Logging {

    //TODO validar este metodo
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