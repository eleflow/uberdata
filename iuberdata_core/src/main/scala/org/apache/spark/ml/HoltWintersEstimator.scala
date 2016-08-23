package org.apache.spark.ml

import com.cloudera.sparkts.models.TimeSeriesModel

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType


/**
  * Created by dirceu on 19/05/16.
  */
class HoltWintersBestModel[T, M <: TimeSeriesModel](override val uid: String,
                                                    val bestPrediction: RDD[(T, M)]
                                                    , val validationMetrics: RDD[(T, ModelParamEvaluation[T])])
  extends Model[HoltWintersBestModel[T, M]] with TimeSeriesBestModelFinderParam[T] {

  //TODO look for this method usage to see if it can be removed
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): HoltWintersBestModel[T, M] = {
    val copied = new HoltWintersBestModel[T, M](
      uid,
      bestPrediction,
      validationMetrics
    )
    copyValues(copied, extra)
  }
}