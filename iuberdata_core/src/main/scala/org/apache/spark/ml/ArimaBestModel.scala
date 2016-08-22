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
class ArimaBestModel[L, M <: TimeSeriesModel](override val uid: String,
                                              val bestPrediction: RDD[(L, M)]
                                              , val validationMetrics: RDD[(L, Seq[ModelParamEvaluation[L]])])
  extends Model[ArimaBestModel[L, M]] with TimeSeriesBestModelFinderParam[L] {

  //TODO avaliar necessidade
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    dataset
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): ArimaBestModel[L, M] = {
    val copied = new ArimaBestModel[L, M](
      uid,
      bestPrediction,
      validationMetrics
    )
    copyValues(copied, extra)
  }
}