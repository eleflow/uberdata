package org.apache.spark.ml

import eleflow.uberdata.core.data.DataTransformer
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.models.UberXGBOOSTModel
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

import scala.reflect.ClassTag

/**
  * Created by dirceu on 29/06/16.
  */
class XGBoost[T](override val uid: String, val models: RDD[(T, (UberXGBOOSTModel, Seq[(ModelParamEvaluation[T])]))])
                (implicit kt: ClassTag[T], ord: Ordering[T] = null) extends ForecastBaseModel[XGBoostModel[T]]
  with HasInputCol with HasOutputCol with HasLabelCol with DefaultParamsWritable with HasFeaturesCol with HasNFutures {

  def this(models: RDD[(T, (UberXGBOOSTModel, Seq[(ModelParamEvaluation[T])]))])(implicit kt: ClassTag[T],
                                                                                 ord: Ordering[T] = null) =
    this(Identifiable.randomUID("xgboost"), models)

  override def transform(dataSet: DataFrame): DataFrame = {
    val schema = dataSet.schema
    val predSchema = transformSchema(schema) //TODO mudar pra groupbycol

//    val scContext = dataSet.sqlContext.sparkContext
    //TODO fazer com que os modelos invalidos voltem numeros absurdos

    val joined = models.join(dataSet.map(r => (r.getAs[T]($(featuresCol)), r)))

    val featuresColName = dataSet.sqlContext.sparkContext.broadcast($(featuresCol))
//    val nFut = scContext.broadcast($(nFutures))
    val predictions = joined.map {
      case (id, ((bestModel, metrics), row)) =>
        val features = row.getAs[Array[org.apache.spark.mllib.linalg.Vector]]("features")
        val label = DataTransformer.toFloat(row.getAs($(labelCol)))
        val labelPoint = features.map {
          vec =>
            val array = vec.toArray.map(_.toFloat)
            LabeledPoint.fromDenseVector(label, array)
        }
        val matrix = new DMatrix(labelPoint.toIterator)
        val (ownFeaturesPrediction, forecast) = bestModel.boosterInstance.predict(matrix).flatMap(_.map(_.toDouble)).splitAt(features.length)
        Row(row.toSeq :+ Vectors.dense(forecast) :+ SupportedAlgorithm.Arima.toString :+ bestModel.params.map(f =>
          f._1 -> f._2.toString) :+ Vectors.dense(ownFeaturesPrediction): _*)
    }
    dataSet.sqlContext.createDataFrame(predictions, predSchema)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField($(outputCol), ArrayType(DoubleType)))
  }

  override def copy(extra: ParamMap): XGBoostModel[T] = defaultCopy(extra)
}
