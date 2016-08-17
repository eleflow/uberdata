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
class ArimaBestModel[I, M <: TimeSeriesModel](override val uid: String,
                                              val bestPrediction: RDD[(I, M)]
                                              , val validationMetrics: RDD[(I, Seq[ModelParamEvaluation[I]])])
  extends Model[ArimaBestModel[I, M]] with TimeSeriesBestModelFinderParam[I] {

  //TODO avaliar necessidade
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    dataset
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): ArimaBestModel[I, M] = {
    val copied = new ArimaBestModel[I, M](
      uid,
      bestPrediction,
      validationMetrics
    )
    copyValues(copied, extra)
  }
}