package org.apache.spark.ml

import com.cloudera.sparkts.models.TimeSeriesModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType


/**
  * Created by dirceu on 25/04/16.
  * Chama o bestmodelfinder para achar o melhor modelo
  * e retreina o dataset com todos os dados no melhor modelo
  */
class ArimaBestModel[T, M <: TimeSeriesModel](override val uid: String,
                                              val bestPrediction: RDD[(T, M)]
                                              , val validationMetrics: RDD[(T, Seq[ModelParamEvaluation[T]])])
  extends Model[ArimaBestModel[T, M]] with TimeSeriesBestModelFinderParam[T] {

  //TODO avaliar necessidade
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    dataset
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): ArimaBestModel[T, M] = {
    val copied = new ArimaBestModel[T, M](
      uid,
      bestPrediction,
      validationMetrics
    )
    copyValues(copied, extra)
  }
}