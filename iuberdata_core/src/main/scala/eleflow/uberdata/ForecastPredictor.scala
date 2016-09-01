/*
 * Copyright 2015 eleflow.com.br.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eleflow.uberdata

import java.sql.Timestamp

import eleflow.uberdata.core.exception.UnexpectedValueException
import eleflow.uberdata.enums.SupportedAlgorithm._
import org.apache.spark.Logging
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.TimeSeriesEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.reflect.ClassTag

/**
  * Created by celio on 11/04/16.
  */
object ForecastPredictor {
  def apply(): ForecastPredictor = new ForecastPredictor
}

class ForecastPredictor extends Serializable with Logging {

  lazy val defaultRange = (0 to 2).toArray

  trait TimestampOrd extends Ordering[Timestamp] {
    override def compare(x: Timestamp, y: Timestamp): Int =
      if (x.getTime < y.getTime) -1
      else if (x.getTime == y.getTime) 0
      else 1
  }

  implicit object TimestampOrdering extends TimestampOrd

  protected def defaultARIMAParamMap[T <: ArimaParams](estimator: T, paramRange: Array[Int]) =
    new ParamGridBuilder()
      .addGrid(estimator.arimaP, paramRange)
      .addGrid(estimator.arimaQ, paramRange)
      .addGrid(estimator.arimaD, paramRange)
      .build()
      .filter(f =>
        f.get[Int](estimator.arimaP).getOrElse(0) != 0 ||
          f.get[Int](estimator.arimaQ).getOrElse(0) != 0)

  def prepareARIMAPipeline[L](
    groupByCol: String,
    labelCol: String = "label",
    validationCol: String = "validation",
    timeCol: String = "Date",
    nFutures: Int,
    paramRange: Array[Int] = defaultRange
  )(implicit kt: ClassTag[L]): Pipeline = {

    val transformer =
      createTimeSeriesGenerator[L](groupByCol, labelCol, timeCol)
    prepareARIMAPipelineInt[L](
      groupByCol,
      labelCol,
      validationCol,
      nFutures,
      paramRange,
      Array(transformer))
  }

  val metricName: String = "rmspe"

  protected def prepareARIMAPipelineInt[L](
    groupBycol: String,
    labelCol: String,
    validationCol: String,
    nFutures: Int,
    paramRange: Array[Int],
    transformer: Array[Transformer]
  )(implicit kt: ClassTag[L]) = {
    val timeSeriesEvaluator: TimeSeriesEvaluator[L] =
      new TimeSeriesEvaluator[L]()
        .setValidationCol(validationCol)
        .setLabelCol(labelCol)
        .setMetricName(metricName)
    val arima = new ArimaBestModelFinder[L]()
      .setTimeSeriesEvaluator(timeSeriesEvaluator)
      .setGroupByCol(groupBycol)
      .setValidationCol(validationCol)
      .setNFutures(nFutures)
    val paramGrid = defaultARIMAParamMap[ArimaBestModelFinder[L]](arima, paramRange)
    val arimaBestModelFinder: ArimaBestModelFinder[L] = arima.setEstimatorParamMaps(paramGrid)
    preparePipeline(arimaBestModelFinder, preTransformers = transformer)
  }

  def prepareHOLTWintersPipeline[T](
    groupByCol: String,
    labelCol: String = "label",
    validationCol: String = "validation",
    timeCol: String = "Date",
    nFutures: Int = 6
  )(implicit kt: ClassTag[T]): Pipeline = {
    val transformer = createTimeSeriesGenerator(groupByCol, labelCol, timeCol)
    val timeSeriesEvaluator: TimeSeriesEvaluator[T] =
      new TimeSeriesEvaluator[T]()
        .setValidationCol(validationCol)
        .setLabelCol(labelCol)
        .setMetricName(metricName)
    val holtWinters = new HoltWintersBestModelFinder[T]()
      .setTimeSeriesEvaluator(timeSeriesEvaluator)
      .setGroupByCol(groupByCol)
      .setLabelCol(labelCol)
      .setValidationCol(validationCol)
      .setNFutures(nFutures)
      .asInstanceOf[HoltWintersBestModelFinder[Double]]

    preparePipeline(holtWinters, preTransformers = Array(transformer))
  }

  def prepareMovingAveragePipeline[G](
    groupBycol: String,
    featuresCol: String = "features",
    validationCol: String = "validation",
    timeCol: String = "Date",
    windowSize: Int = 8
  )(implicit kt: ClassTag[G]): Pipeline = {
    val transformer = createTimeSeriesGenerator(groupBycol, featuresCol, timeCol)
    val movingAverage = new MovingAverage[G]()
      .setOutputCol(validationCol)
      .setInputCol(featuresCol)
      .setWindowSize(windowSize)

    new Pipeline().setStages(Array(transformer, movingAverage))
  }

  private def createTimeSeriesGenerator[L](
    groupByCol: String,
    featuresCol: String,
    timeCol: String
  )(implicit kt: ClassTag[L]): TimeSeriesGenerator[L] = {
    new TimeSeriesGenerator[L]()
      .setFeaturesCol(featuresCol)
      .setGroupByCol(groupByCol)
      .setTimeCol(timeCol)
      .setOutputCol("features")
  }

  private def preparePipeline(timeSeriesBestModelFinder: TimeSeriesBestModelFinder,
                              preTransformers: Array[_ <: Transformer]): Pipeline = {

    new Pipeline().setStages(preTransformers ++ Array(timeSeriesBestModelFinder))
  }

  def prepareBestForecastPipeline[L](
    labelCol: String,
    featuresCol: String,
    validationCol: String,
    timeCol: String,
    nFutures: Int,
    meanAverageWindowSize: Seq[Int],
    paramRange: Array[Int]
  )(implicit kt: ClassTag[L]): Pipeline = {
    val transformer =
      createTimeSeriesGenerator[L](labelCol, featuresCol, timeCol)
    val timeSeriesEvaluator: TimeSeriesEvaluator[L] =
      new TimeSeriesEvaluator[L]()
        .setValidationCol(validationCol)
        .setLabelCol(featuresCol)
        .setMetricName(metricName)
    val findBestForecast = new ForecastBestModelFinder[L, ForecastBestModel[L]]
      .setWindowParams(meanAverageWindowSize)
      .setTimeSeriesEvaluator(timeSeriesEvaluator)
      .setLabelCol(labelCol)
      .setValidationCol(validationCol)
      .setNFutures(nFutures)
      .asInstanceOf[ForecastBestModelFinder[L, ForecastBestModel[L]]]
    val paramGrid = defaultARIMAParamMap[ForecastBestModelFinder[L, ForecastBestModel[L]]](
      findBestForecast,
      paramRange)
    findBestForecast.setEstimatorParamMaps(paramGrid)
    preparePipeline(findBestForecast, Array(transformer))
  }

  def prepareXGBoostSmallModel[L, G](
    labelCol: String,
    featuresCol: Seq[String],
    validationCol: String,
    timeCol: String,
    idCol: String,
    groupByCol: String,
    schema: StructType
  )(implicit kl: ClassTag[L], kg: ClassTag[G]): Pipeline = {
    val timeSeriesEvaluator: TimeSeriesEvaluator[G] =
      new TimeSeriesEvaluator[G]()
        .setValidationCol(validationCol)
        .setLabelCol(labelCol)
        .setMetricName(metricName)
    val xgboost = new XGBoostBestSmallModelFinder[L, G]()
      .setTimeSeriesEvaluator(timeSeriesEvaluator)
      .setLabelCol(labelCol)
      .setGroupByCol(groupByCol)
//			.setIdCol(idCol)
      .setValidationCol(validationCol)

    new Pipeline().setStages(
      smallModelPipelineStages(labelCol, featuresCol, groupByCol, Some(idCol), schema = schema)
        :+ xgboost)
  }

  def smallModelPipelineStages(labelCol: String,
                               featuresCol: Seq[String],
                               groupByCol: String,
                               idCol: Option[String] = None,
                               schema: StructType): Array[PipelineStage] = {

    val allColumns = schema.map(_.name).toArray

    val stringColumns = schema
      .filter(f => f.dataType.isInstanceOf[StringType] && featuresCol.contains(f.name))
      .map(_.name)

    val nonStringColumns = allColumns.filter(
      f =>
        !stringColumns.contains(f)
          && featuresCol.contains(f))

    val stringIndexers = stringColumns.map { column =>
      new StringIndexer().setInputCol(column).setOutputCol(s"${column}Index")
    }.toArray

    val nonStringIndex = "nonStringIndex"
    val columnIndexers = new VectorizeEncoder()
      .setInputCol(nonStringColumns)
      .setOutputCol(nonStringIndex)
      .setLabelCol(labelCol)
      .setGroupByCol(groupByCol)
      .setIdCol(idCol.getOrElse(""))

    val assembler = new VectorAssembler()
      .setInputCols(stringColumns.map(f => s"${f}Index").toArray :+ nonStringIndex)
      .setOutputCol(IUberdataForecastUtil.FEATURES_COL_NAME)

    stringIndexers :+ columnIndexers :+ assembler
  }

  //label, GroupBy
  def predict[L, G](train: DataFrame,
                    test: DataFrame,
                    labelCol: String,
                    featuresCol: Seq[String] = Seq.empty[String],
                    timeCol: String,
                    idCol: String,
                    groupByCol: String,
                    algorithm: Algorithm = FindBestForecast,
                    nFutures: Int = 6,
                    meanAverageWindowSize: Seq[Int] = Seq(8, 16, 26),
                    paramRange: Array[Int] = defaultRange)(
    implicit kt: ClassTag[L],
    ord: Ordering[L] = null,
    gt: ClassTag[G]): (DataFrame, PipelineModel) = {
    require(featuresCol.nonEmpty, "featuresCol parameter can't be empty")
    val validationCol = idCol + algorithm.toString
    algorithm match {
      case Arima | HoltWinters | MovingAverage8 | MovingAverage16 | MovingAverage26 |
          FindBestForecast =>
        predictSmallModelFuture[L](
          train,
          test,
          groupByCol,
          featuresCol.head,
          timeCol,
          idCol,
          algorithm,
          validationCol,
          nFutures,
          meanAverageWindowSize,
          paramRange)
      case XGBoostAlgorithm =>
        predictSmallModelFeatureBased[L, G](
          train,
          test,
          labelCol,
          featuresCol,
          timeCol,
          idCol,
          groupByCol,
          algorithm,
          validationCol)
      case _ =>
        throw new UnexpectedValueException(
          s"Algorithm $algorithm can't be used to predict a Forecast")
    }
  }

  def predictSmallModelFeatureBased[L, G](
    train: DataFrame,
    test: DataFrame,
    labelCol: String,
    featuresCol: Seq[String],
    timeCol: String,
    idCol: String,
    groupByCol: String,
    algorithm: Algorithm = XGBoostAlgorithm,
    validationCol: String
  )(implicit kt: ClassTag[L], ord: Ordering[L], gt: ClassTag[G]): (DataFrame, PipelineModel) = {
    require(
      algorithm == XGBoostAlgorithm,
      "The accepted algorithm for this method is XGBoostAlgorithm")
    val pipeline = prepareXGBoostSmallModel[L, G](
      labelCol,
      featuresCol,
      validationCol,
      timeCol,
      idCol,
      groupByCol,
      train.schema)
    val cachedTrain = train.cache
    val cachedTest = test.cache()
    val model = pipeline.fit(cachedTrain)
    val result = model.transform(cachedTest).cache
    val joined = result.select(idCol, IUberdataForecastUtil.FEATURES_PREDICTION_COL_NAME, groupByCol, timeCol)
    val dfToBeReturned = joined.withColumnRenamed("featuresPrediction", "prediction")

    (dfToBeReturned.sort(idCol), model)
  }

  def prepareSmallModelPipeline[G](train: DataFrame,
                                   test: DataFrame,
                                   groupByCol: String,
                                   labelCol: String,
                                   timeCol: String,
                                   idCol: String,
                                   algorithm: Algorithm,
                                   validationCol: String,
                                   nFutures: Int,
                                   meanAverageWindowSize: Seq[Int],
                                   paramRange: Array[Int])(implicit ct: ClassTag[G]) = {
    algorithm match {
      case Arima =>
        prepareARIMAPipeline[G](groupByCol, labelCol, validationCol, timeCol, nFutures, paramRange)
      case HoltWinters =>
        prepareHOLTWintersPipeline[G](
          groupByCol,
          labelCol,
          validationCol,
          timeCol,
          nFutures
        )
      case MovingAverage8 =>
        prepareMovingAveragePipeline[G](
          groupByCol,
          labelCol,
          validationCol,
          timeCol,
          8
        )
      case MovingAverage16 =>
        prepareMovingAveragePipeline[G](
          groupByCol,
          labelCol,
          validationCol,
          timeCol,
          16
        )
      case MovingAverage26 =>
        prepareMovingAveragePipeline[G](
          groupByCol,
          labelCol,
          validationCol,
          timeCol,
          26
        )
      case FindBestForecast =>
        prepareBestForecastPipeline[G](
          groupByCol,
          labelCol,
          validationCol,
          timeCol,
          nFutures,
          meanAverageWindowSize,
          paramRange)
      case _ =>
        throw new UnexpectedValueException(
          s"Algorithm $algorithm can't be used to predict a Forecast")
    }
  }

  def predictSmallModelFuture[G](
    train: DataFrame,
    test: DataFrame,
    groupByCol: String,
    labelCol: String,
    timeCol: String,
    idCol: String,
    algorithm: Algorithm = FindBestForecast,
    validationCol: String,
    nFutures: Int = 6,
    meanAverageWindowSize: Seq[Int] = Seq(8, 16, 26),
    paramRange: Array[Int] = defaultRange
  )(implicit kt: ClassTag[G], ord: Ordering[G] = null): (DataFrame, PipelineModel) = {
    require(
      algorithm != XGBoostAlgorithm,
      "The accepted algorithms for this method doesn't include XGBoost")
    val pipeline = prepareSmallModelPipeline(
      train,
      test,
      groupByCol,
      labelCol,
      timeCol,
      idCol,
      algorithm,
      validationCol,
      nFutures,
      meanAverageWindowSize,
      paramRange)
    val cachedTrain = train.cache
    val model = pipeline.fit(cachedTrain)
    val result = model.transform(cachedTrain)
    val timeColIndex = test.columns.indexOf(timeCol)
    val sparkContext = train.sqlContext.sparkContext
    val timeColIndexBc = sparkContext.broadcast(timeColIndex)
    val labelColBc = sparkContext.broadcast(groupByCol)
    val validationColBc = sparkContext.broadcast(validationCol)
    val validationColIndexBc =
      sparkContext.broadcast(result.columns.indexOf(validationCol))
    val labelColIndexBc =
      sparkContext.broadcast(result.columns.indexOf(groupByCol))
    val featuresColIndexBc =
      sparkContext.broadcast(result.columns.indexOf("features"))
    val featuresValidationColIndexBc =
      sparkContext.broadcast(result.columns.indexOf("featuresValidation"))
    val groupedTest = test.rdd
      .groupBy(row => row.getAs[G](labelColBc.value))
      .map {
        case (key, values) =>
          val sort = values.toArray.map { row =>
            IUberdataForecastUtil.convertColumnToLongAddAtEnd(row, timeColIndexBc.value)
          }.sortBy(row => row.getAs[Long](row.size - 1))
          (key, sort)
      }
      .cache
    val keyValueResult = result.rdd
      .map(
        row =>
          (row.getAs[G](labelColBc.value),
           (row
              .getAs[org.apache.spark.mllib.linalg.Vector](
                validationColBc.value
              )
              .toArray,
            row))
      )
      .cache
    val forecastResult = keyValueResult.join(groupedTest).flatMap {
      case (key, ((predictions, row), ids)) =>
        val filteredRow = row.schema.zipWithIndex.filter {
          case (value, index) =>
            index != validationColIndexBc.value &&
              index != labelColIndexBc.value && index != featuresColIndexBc.value &&
              index != featuresValidationColIndexBc.value && value.name != "featuresPrediction"
        }
        ids.zip(predictions).map {
          case (id, prediction) =>
            val seq = id.toSeq
            val (used, _) = seq.splitAt(seq.length - 1)
            Row(
              used ++ filteredRow.map { case (_, index) => row.get(index) } :+ Math.round(
                prediction): _*
            )
        }
    }
    val sqlContext = train.sqlContext
    val schema = result.schema.fields
      .filter(
        f =>
          f.name != validationCol && f.name != groupByCol && f.name != "features"
            && f.name != "featuresValidation" && f.name != "featuresPrediction"
      )
      .foldLeft(test.schema) {
        case (testSchema, field) => testSchema.add(field)
      }
      .add(StructField("prediction", LongType))
    val df = sqlContext.createDataFrame(forecastResult, schema)

    if(idCol.isEmpty)
      (df, model)
    else
      (df.sort(idCol), model)
  }

  def saveResult[T](toBeSaved: RDD[(T, Long)], path: String): Unit = {
    toBeSaved.map {
      case (key, value) => s"$key,$value"
    }.coalesce(1).saveAsTextFile(path)
  }

  def predictBigModelFuture(train: DataFrame,
                            test: DataFrame,
                            algorithm: Algorithm,
                            labelCol: String,
                            idCol: String,
                            featuresCol: Seq[String]): (DataFrame, PipelineModel) = {
    val pipeline = algorithm match {
      case XGBoostAlgorithm => prepareXGBoostBigModel(labelCol, idCol, featuresCol, train.schema)
      case _ => throw new UnsupportedOperationException()
    }
    val model = pipeline.fit(train.cache)
    val predictions = model.transform(test).cache
    (predictions.sort(idCol), model)
  }

  def prepareXGBoostBigModel[L, G](
    labelCol: String,
    idCol: String,
    featuresCol: Seq[String],
    schema: StructType)(implicit ct: ClassTag[L], gt: ClassTag[G]): Pipeline = {
    val validationCol: String = "validation"
    val timeSeriesEvaluator: TimeSeriesEvaluator[G] = new TimeSeriesEvaluator[G]()
      .setValidationCol(validationCol)
      .setLabelCol(labelCol)
      .setMetricName("rmspe")
    val xgboost = new XGBoostBestBigModelFinder[L, G]()
      .setTimeSeriesEvaluator(timeSeriesEvaluator)
      .setLabelCol(labelCol)
      .setIdCol(idCol)
      .setValidationCol(validationCol)

    new Pipeline().setStages(
      smallModelPipelineStages(labelCol, featuresCol, "", Some(idCol), schema = schema)
        :+ xgboost)
  }
}
