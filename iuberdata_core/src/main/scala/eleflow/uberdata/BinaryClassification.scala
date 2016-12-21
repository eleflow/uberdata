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

import eleflow.uberdata.enums.SupportedAlgorithm._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, TimeSeriesEvaluator}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.shared.HasXGBoostParams
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.reflect.ClassTag
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


/**
  * Created by caio.martins on 22/09/16.
  */

object BinaryClassification {
  def apply(): BinaryClassification = new BinaryClassification
}

class BinaryClassification {

  def predictUsingWindowsApproach(
                                   train: DataFrame,
                                   test: DataFrame,
                                   algorithm: Algorithm,
                                   labelCol: String,
                                   idCol: String,
                                   featuresCol: Seq[String],
                                   rounds: Int = 2000,
                                   params: Map[String, Any] = Map.empty[String, Any],
                                   trainingWindowSize: Int) : (DataFrame, DataFrame, Double, Double) = {

    val testDataSize = test.count().toInt
    val trainDataSize = train.count().toInt
    val numberOfPredictionsByModelUpdate = testDataSize
    val privateid = "privateid"
    val decile = "decile"

    if(trainingWindowSize >= trainDataSize) {
      throw new IllegalArgumentException( "trainingWindowSize is greater than train dataframe size ")
    }

    if(trainDataSize < testDataSize) {
      throw new IllegalArgumentException( "train dataframe size has to be greater than test dataframe size")
    }

    val orderedTrainDataFrame = dfZipWithIndex(train.orderBy(idCol), 1, privateid)

    var indexes = (1 to (trainDataSize-(testDataSize+trainingWindowSize)+1) by numberOfPredictionsByModelUpdate).toArray

    if(indexes.size == 0) {
      indexes = Array(1)
    }

    val predictionsForTrainingSet = indexes.map { index =>
      val clause1 = privateid + " >= " + index
      val clause2 = privateid + " <= " + (index + trainingWindowSize-1)
      val clause3 = privateid + " > " + (index + trainingWindowSize-1)
      val clause4 = privateid + " <= " + (index + trainingWindowSize + numberOfPredictionsByModelUpdate - 1)
      val trainingPartial = orderedTrainDataFrame.where(clause1).where(clause2).drop(privateid).repartition(1)
      val testPartial = orderedTrainDataFrame.where(clause3).where(clause4).drop(privateid).repartition(1)

      val (predictionsPartial, modelPartial) = predict(trainingPartial, testPartial, algorithm, labelCol, idCol, featuresCol, rounds, params )
      insertDecileColumn(predictionsPartial, idCol, decile)

    }

    val allPredictionsForTrainingSetDF = predictionsForTrainingSet.toSeq.reduce( _.unionAll(_)).withColumnRenamed(idCol, "id1")
    val predictionsForTrainingSetStats = allPredictionsForTrainingSetDF.join(orderedTrainDataFrame, allPredictionsForTrainingSetDF("id1") === orderedTrainDataFrame(idCol)).select(idCol, decile, labelCol)
    val conversionRateDF0 = predictionsForTrainingSetStats.groupBy(decile).agg("y" -> "sum").withColumnRenamed("sum(y)", "soma_convertidos")
    val totalConversions= conversionRateDF0.rdd.map(_(1).asInstanceOf[Long]).reduce(_+_)
    val conversionRateDF = conversionRateDF0.withColumn("conversion_rate", conversionRateDF0("soma_convertidos")/totalConversions).select("decile", "conversion_rate")

    val clause1 = privateid + " > " + (trainDataSize - trainingWindowSize)
    val clause2 = privateid + " <= " + trainDataSize
    val trainDataForPredictionToBeReturned = orderedTrainDataFrame.where(clause1).where(clause2).drop(privateid).repartition(1)
    val (predictionsForTestSet, modelForTestSet) = predict(trainDataForPredictionToBeReturned, test.repartition(1), algorithm, labelCol, idCol, featuresCol, rounds, params)

    val predictionsAndLabels = allPredictionsForTrainingSetDF.join(orderedTrainDataFrame, allPredictionsForTrainingSetDF("id1") === orderedTrainDataFrame(idCol)).select("prediction", labelCol)
    val metricsAUC = new BinaryClassificationMetrics(predictionsAndLabels.rdd.map{case Row(a: Float, b: Long) => (a.toDouble,b.toDouble)})

    (conversionRateDF, predictionsForTestSet, metricsAUC.areaUnderPR, metricsAUC.areaUnderROC)
  }

  def predict(
               train: DataFrame,
               test: DataFrame,
               algorithm: Algorithm,
               labelCol: String,
               idCol: String,
               featuresCol: Seq[String],
               rounds: Int = 2000,
               params: Map[String, Any] = Map.empty[String, Any]): (DataFrame, PipelineModel) = {
    val pipeline = algorithm match {
      case XGBoostAlgorithm =>
        prepareXGBoostBigModel(labelCol, idCol, featuresCol, train.schema, rounds, params)
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
                                    schema: StructType,
                                    rounds: Int,
                                    params: Map[String, Any])(implicit ct: ClassTag[L], gt: ClassTag[G]): Pipeline = {

    val xgboost = new XGBoostBestBigModelFinder[L, G]()
      .setLabelCol(labelCol)
      .setIdCol(idCol)

    new Pipeline().setStages(
      createXGBoostPipelineStages(labelCol, featuresCol, Some(idCol), schema = schema) :+ xgboost)
  }

  def createXGBoostPipelineStages(labelCol: String,
                                  featuresCol: Seq[String],
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

    val encoder = stringColumns.map { column =>
      new OneHotEncoder().setInputCol(s"${column}Index").setOutputCol(s"${column}Encoder")
    }.toArray

    val nonStringIndex = "nonStringIndex"
    val columnIndexers = new VectorizeEncoder()
      .setInputCol(nonStringColumns)
      .setOutputCol(nonStringIndex)
      .setLabelCol(labelCol)
      .setIdCol(idCol.getOrElse(""))

    val assembler = new VectorAssembler()
      .setInputCols(stringColumns.map(f => s"${f}Index").toArray :+ nonStringIndex)
      .setOutputCol(IUberdataForecastUtil.FEATURES_COL_NAME)

    stringIndexers ++ encoder :+ columnIndexers :+ assembler
  }

  private def dfZipWithIndex(
                      df: DataFrame,
                      offset: Int = 1,
                      colName: String = "id",
                      inFront: Boolean = true
                    ) : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
      )
    )
  }

  def insertDecileColumn (
                                   df: DataFrame,
                                   idCol: String,
                                   decile: String
                                 ): DataFrame = {
    val window = Window.orderBy("prediction")
    val predictionsPartialWithInvertedDeciles =  df.withColumn(decile, ntile(10).over(window) ).sort(idCol)

    val predictionsPartialRdd = predictionsPartialWithInvertedDeciles.select(idCol, "prediction", decile).rdd.map {
      case Row(id: Float, prediction: Float, dec: Int) => Row(id, prediction, Math.abs(11 - dec))
    }

    df.sqlContext.createDataFrame(predictionsPartialRdd, StructType(Array(StructField(idCol,FloatType),StructField("prediction", FloatType),StructField(decile,IntegerType))))

  }

}