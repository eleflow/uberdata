package org.apache.spark.ml

import eleflow.uberdata.IUberdataForecastUtil
import org.apache.spark.ml.param.shared.{HasNFutures, HasPredictionCol, HasValidationCol}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types._

/**
  * Created by dirceu on 01/06/16.
  */
trait ForecastPipelineStage extends PipelineStage with HasNFutures with HasPredictionCol with HasValidationCol {

  def setValidationCol(value:String) = set(validationCol,value)

  override def transformSchema(schema: StructType): StructType = {
    schema.add(StructField($(validationCol), new VectorUDT)).add(StructField(IUberdataForecastUtil.ALGORITHM,StringType)).
      add(StructField(IUberdataForecastUtil.PARAMS,MapType(StringType,DoubleType)))
  }
}
