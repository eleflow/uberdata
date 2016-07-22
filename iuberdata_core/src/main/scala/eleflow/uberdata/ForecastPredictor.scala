package eleflow.uberdata

import java.sql.Timestamp

import eleflow.uberdata.core.data.{DataTransformer, Dataset}
import Dataset._
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
    override def compare(x: Timestamp, y: Timestamp): Int = if (x.getTime < y.getTime) -1
    else if (x.getTime == y.getTime) 0
    else 1
  }

  implicit object TimestampOrdering extends TimestampOrd

  protected def defaultARIMAParamMap[T <: ArimaParams](estimator: T, paramRange: Array[Int]) = new ParamGridBuilder()
    .addGrid(estimator.arimaP, paramRange)
    .addGrid(estimator.arimaQ, paramRange)
    .addGrid(estimator.arimaD, paramRange)
    .build().filter(f => f.get[Int](estimator.arimaP).getOrElse(0) != 0 ||
    f.get[Int](estimator.arimaQ).getOrElse(0) != 0)

  def prepareARIMAPipeline[T, U](labelCol: String = "label", featuresCol: String = "features",
                                 validationCol: String = "validation", timeCol: String = "date", nFutures: Int = 6, paramRange: Array[Int] = defaultRange)
                                (implicit kt: ClassTag[T]): Pipeline = {
    val transformer = createTimeSeriesGenerator(labelCol, featuresCol, validationCol, timeCol)
    prepareARIMAPipeline[T](labelCol, featuresCol, validationCol, nFutures, Array(transformer), paramRange)
  }

  /**
    * Testing purpose
    *
    * @return
    */
  protected def prepareARIMAPipeline[T](labelCol: String, featuresCol: String, validationCol: String, nFutures: Int,
                                        transformer: Array[Transformer] = Array.empty[Transformer], paramRange: Array[Int])
                                       (implicit kt: ClassTag[T]) = {
    val arima = new ArimaBestModelFinder[T]().setTimeSeriesEvaluator(
      new TimeSeriesEvaluator[T]().setValidationCol(validationCol)
        .setFeaturesCol(featuresCol).setMetricName("rmspe")).setLabelCol(labelCol).setValidationCol(validationCol)
      .setNFutures(nFutures).asInstanceOf[ArimaBestModelFinder[T]]
    val paramGrid = defaultARIMAParamMap[ArimaBestModelFinder[T]](arima, paramRange)
    preparePipeline(arima.setEstimatorParamMaps(paramGrid).asInstanceOf[ArimaBestModelFinder[T]],
      preTransformers = transformer)
  }

  def prepareHOLTWintersPipeline[T, U](labelCol: String = "label", featuresCol: String = "features",
                                       validationCol: String = "validation", timeCol: String = "date", nFutures: Int = 6)
                                      (implicit kt: ClassTag[T]): Pipeline = {
    val transformer = createTimeSeriesGenerator(labelCol, featuresCol, validationCol, timeCol)
    val holtWinters = new HoltWintersBestModelFinder[T]().setTimeSeriesEvaluator(
      new TimeSeriesEvaluator[T]().setValidationCol(validationCol)
        .setFeaturesCol(featuresCol).setMetricName("rmspe")).setLabelCol(labelCol).setValidationCol(validationCol)
      .setNFutures(nFutures).asInstanceOf[HoltWintersBestModelFinder[Double]]
    preparePipeline(holtWinters, preTransformers = Array(transformer))
  }

  def prepareMovingAveragePipeline[T, U](labelCol: String = "label", featuresCol: String = "features",
                                         validationCol: String = "validation", timeCol: String = "date",
                                         windowSize: Int = 8)
                                        (implicit kt: ClassTag[T]): Pipeline = {
    val transformer = createTimeSeriesGenerator(labelCol, featuresCol, validationCol, timeCol)
    val movingAverage = new MovingAverage[T]().setLabelCol(labelCol).setOutputCol(validationCol).setInputCol(featuresCol)
      .setWindowSize(windowSize)
    new Pipeline()
      .setStages(Array(transformer, movingAverage))

  }

  private def createTimeSeriesGenerator[T, U](labelCol: String, featuresCol: String, validationCol: String,
                                              timeCol: String)(implicit kt: ClassTag[T])
  : TimeSeriesGenerator[T, U] = {
    new TimeSeriesGenerator[T, U]().setFeaturesCol(featuresCol).setLabelCol(labelCol).setTimeCol(timeCol)
      .setOutputCol("features")
  }

  private def createAllColumnsTimeSeriesGenerator[T, U](labelCol: String, featuresCol: String, validationCol: String,
                                                        timeCol: String, nFutures: Int)(implicit kt: ClassTag[T])
  : AllColumnsTimeSeriesGenerator[T, U] = {
    new AllColumnsTimeSeriesGenerator[T, U]().setFeaturesCol(featuresCol).setLabelCol(labelCol).setTimeCol(timeCol)
      .setOutputCol("features")
  }

  private def preparePipeline(timeSeriesBestModelFinder: TimeSeriesBestModelFinder,
                              preTransformers: Array[_ <: Transformer]): Pipeline = {
    new Pipeline()
      .setStages(preTransformers ++ Array(timeSeriesBestModelFinder))
  }

  def prepareBestForecastPipeline[T, U](labelCol: String, featuresCol: String, validationCol: String, timeCol: String,
                                        nFutures: Int, meanAverageWindowSize: Seq[Int], paramRange: Array[Int])
                                       (implicit kt: ClassTag[T]): Pipeline = {
    val transformer = createTimeSeriesGenerator[T, U](labelCol, featuresCol, validationCol, timeCol)
    val findBestForecast = new ForecastBestModelFinder[T, ForecastBestModel[T]].setWindowParams(meanAverageWindowSize)
      .setTimeSeriesEvaluator(new TimeSeriesEvaluator[T]().setValidationCol(validationCol).setFeaturesCol(featuresCol)
        .setMetricName("rmspe")).setLabelCol(labelCol).setValidationCol(validationCol).setNFutures(nFutures)
      .asInstanceOf[ForecastBestModelFinder[T, ForecastBestModel[T]]]

    val paramGrid = defaultARIMAParamMap[ForecastBestModelFinder[T, ForecastBestModel[T]]](findBestForecast, paramRange)
    findBestForecast.setEstimatorParamMaps(paramGrid)
    preparePipeline(findBestForecast, Array(transformer))
  }

  def prepareXGBoost[T, L](featuresCol: String, groupByCol: String, validationCol: String, timeCol: String,
                           schema: StructType)
                          (implicit kt: ClassTag[T]) = {
    val xgboost = new XGBoostBestModelFinder[T, L]().setTimeSeriesEvaluator(new TimeSeriesEvaluator[T]().setValidationCol(validationCol)
      .setFeaturesCol(featuresCol).setMetricName("rmspe")).setGroupByCol(groupByCol).setFeaturesCol(featuresCol)
      .setValidationCol(validationCol)

    new Pipeline()
      .setStages(smallModelPipelineStages(featuresCol, groupByCol, timeCol, schema = schema) :+ xgboost)
  }

  def smallModelPipeline(labelCol: String, featuresCol: String, timeCol: String, idCol: String,
                         schema: StructType) =
    new Pipeline().setStages(smallModelPipelineStages(labelCol, featuresCol, timeCol, Some(idCol), schema))

  def smallModelPipelineStages(featuresCol: String, groupByCol: String, timeCol: String, idCol: Option[String] = None,
                               schema: StructType): Array[PipelineStage] = {
    val stringColumns = schema.filter(f => f.dataType.isInstanceOf[StringType] && f.name != groupByCol)
      .map(_.name)
    val allColumns = schema.map(_.name).toArray
    val nonStringColumns = allColumns.filter(f => !stringColumns.contains(f)
      && f != featuresCol && f != idCol.getOrElse("") && f != groupByCol)
    val stringIndexers = stringColumns.map {
      column => new StringIndexer().setInputCol(column).setOutputCol(s"${column}Index")
    }.toArray
    val columnIndexers = new VectorizerEncoder().setInputCol(nonStringColumns).setOutputCol("nonStringIndex")
      .setFeaturesCol(featuresCol).setGroupByCol(groupByCol).setIdCol(idCol.getOrElse(""))
    val assembler = new VectorAssembler()
      .setInputCols(stringColumns.map(f => s"${f}Index").toArray :+ "nonStringIndex")
      .setOutputCol(IUberdataForecastUtil.FEATURES_COL_NAME)
    stringIndexers :+ columnIndexers :+ assembler
  }

  //retornar data, label, p
  // redicao, colocar parametros como argumento
  //retornar o test com mais uma coluna de predicao, mais algoritmo e parametros
  //label, timecol, Id
  def predict[L, T, I](train: DataFrame, test: DataFrame, labelCol: String, featuresCol: String, timeCol: String,
                       idCol: String, algorithm: Algorithm = FindBestForecast, nFutures: Int = 6,
                       meanAverageWindowSize: Seq[Int] = Seq(8, 16, 26), paramRange: Array[Int] = defaultRange)
                      (implicit kt: ClassTag[L], ord: Ordering[L] = null, ctLabel: ClassTag[I],
                       ordLabel: Ordering[I] = null) = {
    val validationCol = idCol + algorithm.toString
    algorithm match {
      case Arima | HoltWinters | MovingAverage8 | MovingAverage16 | MovingAverage26 | FindBestForecast=> predictSmallModelFuture[L, T, I](
        train, test, labelCol, featuresCol, timeCol, idCol, algorithm, validationCol, nFutures, meanAverageWindowSize,
        paramRange)
      case XGBoostAlgorithm => predictSmallModelFeatureBased[L, T, I](train, test, labelCol, featuresCol, timeCol, idCol
        , algorithm, validationCol)
        //TODO missing FindBestForecast for both xgboost and arima
//      case FindBestForecast => prepareBestForecastPipeline[L, T](labelCol, featuresCol, validationCol, timeCol, nFutures,
//         meanAverageWindowSize, paramRange)
      case _ => ???
    }
  }

  def predictSmallModelFeatureBased[L, T, I](train: DataFrame, test: DataFrame, featuresCol: String, groupByCol: String, timeCol: String,
                                             idCol: String, algorithm: Algorithm = FindBestForecast, validationCol: String)
                                            (implicit kt: ClassTag[L], ord: Ordering[L] = null, ctLabel: ClassTag[I],
                                             ordLabel: Ordering[I] = null) = {
    require(algorithm == XGBoostAlgorithm || algorithm == FindBestForecast, "The accepted algorithms for this method are " +
      "XGBoost or FindBest")
    val pipeline = algorithm match {
      case XGBoostAlgorithm => prepareXGBoost[L, T](featuresCol, groupByCol, validationCol, timeCol, train.schema)
      //      case FindBestForecast => prepareBestForecastPipeline[L, T](labelCol, featuresCol, validationCol, timeCol, nFutures,
      //        meanAverageWindowSize, paramRange)
      case _ => ???
    }
    val cachedTrain = train.cache
    val cachedTest = test.cache()
    val model = pipeline.fit(cachedTrain)
    val result = model.transform(cachedTest).cache
    val testPipeline = smallModelPipeline(featuresCol, groupByCol, timeCol, idCol, test.schema)
    val pipelineModel = testPipeline.fit(test)
    val groupedTest = pipelineModel.transform(test).map {
      row =>
        val features = row.getAs[org.apache.spark.mllib.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME)
        val store = row.getAs[T](groupByCol)
        ((features, store), row.getAs[I](idCol))
    }
    val resultColumns = result.select(groupByCol, IUberdataForecastUtil.FEATURES_COL_NAME,
      IUberdataForecastUtil.FEATURES_PREDICTION_COL_NAME).map {
      row => ((row.getAs[org.apache.spark.mllib.linalg.Vector](IUberdataForecastUtil.FEATURES_COL_NAME),
        row.getAs[T](groupByCol)),row.getAs[Double](IUberdataForecastUtil.FEATURES_PREDICTION_COL_NAME))
    }
    val joined = resultColumns.join(groupedTest).map {
      case (features, (prediction, id)) =>
        Row(DataTransformer.toDouble(id), prediction)
    }
    val sqlContext = train.sqlContext
    val schema = StructType(Seq(StructField(idCol, DoubleType), StructField("prediction", DoubleType)))
    val df: Dataset = sqlContext.createDataFrame(joined, schema)
    (df.sort(idCol), model)
  }

  def predictSmallModelFuture[L, T, I](train: DataFrame, test: DataFrame, labelCol: String, featuresCol: String, timeCol: String,
                                       idCol: String, algorithm: Algorithm = FindBestForecast, validationCol: String, nFutures: Int = 6,
                                       meanAverageWindowSize: Seq[Int] = Seq(8, 16, 26), paramRange: Array[Int] = defaultRange)
                                      (implicit kt: ClassTag[L], ord: Ordering[L] = null, ctLabel: ClassTag[I],
                                       ordLabel: Ordering[I] = null) = {
    require(algorithm != XGBoostAlgorithm, "The accepted algorithms for this method doesn't include XGBoost")
    val pipeline = algorithm match {
      case Arima =>
        prepareARIMAPipeline[L, T](labelCol, featuresCol, validationCol, timeCol, nFutures, paramRange)
      case HoltWinters => prepareHOLTWintersPipeline[L, T](labelCol, featuresCol, validationCol, timeCol, nFutures)
      case MovingAverage8 => prepareMovingAveragePipeline[L, T](labelCol, featuresCol, validationCol, timeCol, 8)
      case MovingAverage16 => prepareMovingAveragePipeline[L, T](labelCol, featuresCol, validationCol, timeCol, 16)
      case MovingAverage26 => prepareMovingAveragePipeline[L, T](labelCol, featuresCol, validationCol, timeCol, 26)
      case FindBestForecast => prepareBestForecastPipeline[L, T](labelCol, featuresCol, validationCol, timeCol, nFutures,
        meanAverageWindowSize, paramRange)
      case _ => ???
    }
    val cachedTrain = train.cache
    val model = pipeline.fit(cachedTrain)
    val result = model.transform(cachedTrain)
    val timeColIndex = test.columns.indexOf(timeCol)
    val sparkContext = train.sqlContext.sparkContext
    val timeColIndexBc = sparkContext.broadcast(timeColIndex)
    val labelColBc = sparkContext.broadcast(labelCol)
    val validationColBc = sparkContext.broadcast(validationCol)
    val validationColIndexBc = sparkContext.broadcast(result.columns.indexOf(validationCol))
    val labelColIndexBc = sparkContext.broadcast(result.columns.indexOf(labelCol))
    val featuresColIndexBc = sparkContext.broadcast(result.columns.indexOf("features"))
    val featuresValidationColIndexBc = sparkContext.broadcast(result.columns.indexOf("featuresValidation"))
    val groupedTest = test.rdd.groupBy(row => row.getAs[L](labelColBc.value)).map { case (key, values) =>
      val sort = values.toArray.map {
        row => IUberdataForecastUtil.convertColumnToLongAddAtEnd(row, timeColIndexBc.value)
      }.sortBy(row => row.getAs[Long](row.size - 1))
      (key, sort)
    }.cache
    val keyValueResult = result.rdd.map(row =>

      (row.getAs[L](labelColBc.value), (row
        .getAs[org.apache.spark.mllib.linalg.Vector](validationColBc.value).toArray, row)
        )).cache
    val forecastResult = keyValueResult.join(groupedTest).flatMap {
      case (key, ((predictions, row), ids)) =>
        val filteredRow = row.schema.zipWithIndex.filter { case (value, index) => index != validationColIndexBc.value &&
          index != labelColIndexBc.value && index != featuresColIndexBc.value &&
          index != featuresValidationColIndexBc.value && value.name != "featuresPrediction"
        }
        ids.zip(predictions).map {
          case (id, prediction) =>
            val seq = id.toSeq
            val (used, _) = seq.splitAt(seq.length - 1)
            Row(used ++ filteredRow.map { case (_, index) => row.get(index) } :+ Math.round(prediction): _*)
        }
    }
    val sqlContext = train.sqlContext
    val schema = result.schema.fields.filter(f => f.name != validationCol && f.name != labelCol && f.name != "features"
      && f.name != "featuresValidation" && f.name != "featuresPrediction").foldLeft(test.schema) {
      case (testSchema, field) => testSchema.add(field)
    }.add(StructField("prediction", LongType))
    val df = sqlContext.createDataFrame(forecastResult, schema)
    (df.sort(idCol), model)
  }

  def saveResult[T](toBeSaved: RDD[(T, Long)], path: String) = {
    toBeSaved.map {
      case (key, value) => s"$key,$value"
    }.coalesce(1).saveAsTextFile(path)
  }
}
