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

package eleflow.uberdata.data

import java.io.Serializable

import eleflow.uberdata.core.data.{DataTransformer, Dataset}
import eleflow.uberdata.core.enums.DataSetType
import eleflow.uberdata.core.exception.InvalidDataException
import eleflow.uberdata.data.stat.Statistics
import eleflow.uberdata.enums.ValidationMethod
import eleflow.uberdata.enums.SupportedAlgorithm._
import eleflow.uberdata.model.{Step, TypeMixin}
import eleflow.uberdata.model.TypeMixin._
//import org.apache.spark.Logging
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.mllib.classification.ANNClassifierModel
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter
import ValidationMethod._
import org.apache.spark.SparkContext._

import scala.concurrent.forkjoin.ForkJoinPool
import org.apache.spark.mllib.feature.StandardScaler

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Random

/**
  * Created by dirceu on 21/10/14.
  */
object Predictor extends Predictor

//trait Predictor extends Serializable with Logging {
trait Predictor extends Serializable {

  import DataTransformer._

  type Models = TrainedData[
    _ >: SVMModel with LogisticRegressionModel with NaiveBayesModel with RegressionModel with ANNClassifierModel <: Serializable
  ]

  import Dataset._

  def validate(rdd: RDD[(Double, Double)], method: ValidationMethod): Double = {
    method match {
      case LogarithmicLoss => Statistics.logLoss(rdd)
      case AreaUnderTheROC =>
        new BinaryClassificationMetrics(rdd.map(_.swap)).areaUnderROC()
      case _ => ???
    }
  }

  def evolutiveSVMPredict(trainDataset: Dataset,
                          test: Dataset,
                          targetName: Seq[String],
                          idName: Seq[String],
                          method: ValidationMethod,
                          iterations: Int,
                          algorithm: Algorithm,
                          validationPercentage: Double = 0.3d,
                          columnsToUse: Int = 10) = {
    val slf4jLogger: Logger  = LoggerFactory.getLogger(Dataset.getClass);

    val (
    trainDataSetCached,
    filteredTrainDataSetCached,
    validationDataSet,
    testDataSet
    ) =
      buildTestAndTrainDataSet(
        trainDataset,
        test,
        targetName,
        idName,
        validationPercentage
      )
    val svmModel = train(
      trainDataSetCached.values,
      filteredTrainDataSetCached,
      targetName,
      BinarySupportVectorMachines,
      iterations
    ).model.asInstanceOf[SVMModel]
    val columns = svmModel.weights.toArray.zipWithIndex
      .sortBy(_._1)(Ordering[Double].reverse)
      .take(columnsToUse)
    slf4jLogger.info(s"Using columns weight of ${columns.map(_._1)} and columns ids ${columns.map(_._2)}")
    val (trainlp, validationlp, testlp) = extractColumnsFromLP(
      columns,
      trainDataSetCached.values,
      validationDataSet.values,
      testDataSet
    )
    val model = train(
      trainlp,
      filteredTrainDataSetCached,
      targetName,
      algorithm,
      iterations
    )
    val validationPredict = executePredict(model, validationlp)
    val testPredict = executePredict(model, testlp.map(_._2).cache)

    (validationPredict,
     model,
     testDataSet,
     validationDataSet,
     trainDataSetCached,
     testPredict,
     testDataSet.keys)
  }

  def evolutiveMiddleStart(train: Dataset,
                           testSetStd: Dataset,
                           targetName: Seq[String],
                           idName: Seq[String],
                           quantity: Int,
                           method: ValidationMethod,
                           iterations: Int,
                           algorithm: Algorithm,
                           columns: Array[String]) = {
    val featuresSize = train.columns.size - 2
    val middle = (featuresSize / 2)

    val (trainLP, _, validationLP, testLP) =
      buildTestAndTrainDataSet(train, testSetStd, targetName, idName)
    predictEvolutiveMiddleStart(
      middle,
      featuresSize,
      quantity,
      trainLP.values,
      validationLP.values,
      testLP,
      method,
      iterations,
      algorithm,
      columns
    )
  }

  def predictEvolutiveMiddleStart(
    middle: Int,
    featuresSize: Int,
    quantity: Int,
    trainLP: RDD[LabeledPoint],
    validationLP: RDD[LabeledPoint],
    testLP: RDD[((Double, Any), LabeledPoint)],
    method: ValidationMethod,
    iterations: Int,
    algorithm: Algorithm,
    columns: Array[String],
    previousScore: Double = 0.0,
    topScore: Double = 0.0001,
    result: List[(Double, Array[Int])] = List.empty): List[(Double, Array[Int])] = {

    if (previousScore > topScore) result
    else {
      val featuresCombinations =
        (getRandomCombinationsSize(quantity, featuresSize, middle) ++
          getRandomCombinationsSize(quantity, featuresSize, middle + 1) ++
          getRandomCombinationsSize(quantity, featuresSize, middle - 1) ++
          getRandomCombinationsRandomSize(quantity, featuresSize)).par

      featuresCombinations.tasksupport = new ForkJoinTaskSupport(
        new scala.concurrent.forkjoin.ForkJoinPool(4)
      )
      val results = featuresCombinations.map { featureCombination =>
        val newTraining = trainLP
          .map(
            lp =>
              LabeledPoint(
                lp.label,
                org.apache.spark.mllib.linalg.Vectors.fromML(Vectors.dense(getIndices(lp.features.toArray, featureCombination)))
            )
          )
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        val newXValidation = validationLP
          .map(
            lp =>
              LabeledPoint(
                lp.label,
                org.apache.spark.mllib.linalg.Vectors.fromML(Vectors.dense(getIndices(lp.features.toArray, featureCombination)))
            )
          )
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        val (_, result, toSubmit) = partialPredict(
          newTraining,
          newXValidation,
          method,
          iterations,
          algorithm,
          columns
        )

        //      toSubmit.saveAsTextFile(path+"/submission"+hash+".txt")
        val hash = featureCombination.mkString("-").hashCode
        println(s"hash: $hash auc:$result features: $featureCombination")
        (result, (featureCombination.toArray, hash, toSubmit))
      //        (result,featureCombination)
      }
      val scores =
        (TreeMap.empty[Double, (Array[Int], String, RDD[String])] ++ results.toMap).toList.reverse

      println(scores)
      val topScores = scores.slice(0, 10)
      val newTopScore = topScores(0)._1
      //      topScores.map(x => x._2._3.saveAsTextFile(path + "/submission" + x._2._2 + ".txt"))
      println("TOP 10 saved: " + newTopScore)
      val topAverageLength = (0.0 /: topScores.map(_._2._1.length)) {
        _ + _
      } / topScores.size
      println("TOP 10 Average Length: " + topAverageLength)
      val newMiddle =
        if (topAverageLength > middle)
          middle + 1
        else
          middle - 1
      println(s"Next middle is: $newMiddle")
      println(s"top score $topScore")
      println(s"prev score $previousScore")
      val top = topScores.map(f => (f._1, f._2._1))
      predictEvolutiveMiddleStart(
        newMiddle,
        featuresSize,
        quantity,
        trainLP,
        validationLP,
        testLP,
        method,
        iterations,
        algorithm,
        columns,
        topScore,
        newTopScore,
        top ++ result
      )
    }

  }

  def getRandomCombinationsSize(quantity: Int, m: Int, n: Int) = {
    (for (i <- (1 to quantity))
      yield (Random.shuffle((0 to (m - 1)).toList).combinations(n).next))
  }

  def getRandomCombinationsRandomSize(quantity: Int, m: Int) = {
    (for (i <- (1 to quantity))
      yield (Random.shuffle((0 to (m - 1)).toList).combinations(Random.nextInt(m - 1) + 1).next))
  }

  def getIndices(array: Array[Double], indices: Seq[Int]) = {
    indices.map(i => array(i)).toArray
  }

  def partialPredict(
    train: RDD[LabeledPoint],
    validationDataSet: RDD[LabeledPoint],
    method: ValidationMethod,
    iterations: Int,
    algorithm: Algorithm,
    columns: Array[String]
  ): (Models, Double, Array[String]) = {
    val model = trainForAlgorithm(train, algorithm, iterations)

    val prediction = executePredict(model, validationDataSet)
    (model, validate(prediction, method), columns)
  }

  def take(models: Array[(Models, Double, Array[String])], method: ValidationMethod) = {
    val sorted = method match {
      case LogarithmicLoss =>
        models.sortWith(_._2 < _._2)
      case _ => models.sortWith(_._2 > _._2)
    }
    sorted.foldLeft(List.empty[String]) { case (a, b) => a ++ b._3 }
  }

  def evolutivePredictColumnList(
    train: Dataset,
    test: Dataset,
    ids: Seq[String],
    target: Seq[String],
    validationPercentage: Double = 0.3d,
    algorithm: Algorithm,
    validationMethod: ValidationMethod,
    columnNStep: List[Int],
    iterations: Int
  ): Array[(Models, Double, Array[String])] = {

    val steps = new EvolutiveSingleStart(train.schema.fields.size, columnNStep)
    val (cachedTrain, cachedValidation, cachedTest) =
      evolutivePredictObjectSplit(train, test, ids, target)
    val joinedDataSet = prepareToSummarizeColumns(train, test, target, ids)
    evolutivePrediction(
      cachedTrain,
      cachedValidation,
      cachedTest,
      ids,
      target,
      validationPercentage,
      algorithm,
      validationMethod,
      steps.steps,
      iterations,
      List.empty[String],
      Array.empty[(Models, Double, Array[String])],
      joinedDataSet
    )
  }

  def evolutivePredict(
    train: Dataset,
    test: Dataset,
    ids: Seq[String] = Seq("id"),
    target: Seq[String] = Seq("target"),
    validationPercentage: Double = 0.3d,
    algorithm: Algorithm,
    validationMethod: ValidationMethod,
    steps: Seq[Step],
    iterations: Int
  ): Array[(Models, Double, Array[String])] = {
    require(!steps.isEmpty, "Steps can't be empty")
    val (cachedTrain, cachedValidation, cachedTest) =
      evolutivePredictObjectSplit(train, test, ids, target)
    val joinedDataSet = prepareToSummarizeColumns(train, test, target, ids)
    val evolutivePredictionResult = evolutivePrediction(
      cachedTrain,
      cachedValidation,
      cachedTest,
      ids,
      target,
      validationPercentage,
      algorithm,
      validationMethod,
      steps,
      iterations,
      List.empty[String],
      Array.empty[(Models, Double, Array[String])],
      joinedDataSet
    )
    cachedTrain.unpersist(false)
    cachedValidation.unpersist(false)
    cachedTest.unpersist(false)
    evolutivePredictionResult
  }

  def predictWithColumnSetupInt(train: Dataset,
                                test: Dataset,
                                ids: Seq[Int] = Seq(0),
                                target: Seq[Int] = Seq.empty[Int],
                                validationPercentage: Double = 0.3d,
                                algorithm: Algorithm,
                                validationMethod: ValidationMethod,
                                selectColumns: Either[Int, Double],
                                iterations: Int) = {
    val (
    filteredTrainDataSet,
    filteredTestDataSet,
    targetIndexes,
    testTargetIndexes
    ) =
      extractDataAndIndexes(
        train,
        test,
        ids,
        target,
        train.toDataFrame.schema.fields.zipWithIndex.map(_._2),
        Seq.empty
      )
    val (trainDataSetCached, _, validationDataSet, testDataSet) =
      buildTestAndTrainDataSet(
        filteredTrainDataSet,
        filteredTestDataSet,
        targetIndexes,
        testTargetIndexes,
        validationPercentage
      )
    val (trainLabel, validationLabel, testLabel, correlatedColumns) =
      Statistics.correlationLabeledPoint(
        trainDataSetCached.values,
        validationDataSet.values,
        testDataSet.values,
        selectColumns
      )
    val model = trainForAlgorithm(trainLabel.cache, algorithm, iterations)
    val validationPrediction = executePredict(model, validationLabel.cache)
    val prediction = executePredict(model, testLabel.cache)
    (validationPrediction,
     model,
     testLabel,
     validationLabel,
     trainLabel,
     prediction,
     correlatedColumns)
  }

  def predictWithColumnSetup(train: Dataset,
                             test: Dataset,
                             ids: Seq[String] = Seq("id"),
                             target: Seq[String] = Seq("target"),
                             validationPercentage: Double = 0.3d,
                             algorithm: Algorithm,
                             validationMethod: ValidationMethod,
                             selectColumns: Either[Int, Double],
                             iterations: Int) = {
    val (
    filteredTrainDataSet,
    filteredTestDataSet,
    targetIndexes,
    testTargetIndexes
    ) =
      extractDataAndIndexesStr(
        train,
        test,
        ids,
        target,
        train.toDataFrame.schema.fields.map(_.name),
        Seq.empty
      )
    val (trainDataSetCached, _, validationDataSet, testDataSet) =
      buildTestAndTrainDataSet(
        filteredTrainDataSet,
        filteredTestDataSet,
        targetIndexes,
        testTargetIndexes,
        validationPercentage
      )
    val (trainLabel, validationLabel, testLabel, correlatedColumns) =
      Statistics.correlationLabeledPoint(
        trainDataSetCached.values,
        validationDataSet.values,
        testDataSet.values,
        selectColumns
      )
    val model = trainForAlgorithm(trainLabel.cache, algorithm, iterations)
    val validationPrediction = executePredict(model, validationLabel.cache)
    val prediction = executePredict(model, testLabel.cache)
    (validationPrediction,
     model,
     testLabel,
     validationLabel,
     trainLabel,
     prediction,
     correlatedColumns)
  }

  //  private
  def executePredict[T](model: TrainedData[T], dataSet: RDD[LabeledPoint]) = {
    model.algorithm match {
      case BinaryLogisticRegressionBFGS | BinaryLogisticRegressionSGD =>
        binaryLogisticRegressionPredict(
          model.model.asInstanceOf[LogisticRegressionModel],
          dataSet
        )
      case BinarySupportVectorMachines =>
        svmPredict(model.model.asInstanceOf[SVMModel], dataSet)
      case NaiveBayesClassifier =>
        naiveBayesPredict(model.model.asInstanceOf[NaiveBayesModel], dataSet)
      case LinearLeastSquares =>
        linearPredict(model.model.asInstanceOf[LassoModel], dataSet)
      case ArtificialNeuralNetwork =>
        neuralNetworkPredict(
          model.model.asInstanceOf[ANNClassifierModel],
          dataSet
        )
    }
  }

  def neuralNetworkPredict(model: ANNClassifierModel, dataSet: RDD[LabeledPoint]) =
    dataSet.map(f => (f.label, model.predict(f.features)))

  def svmPredict(model: SVMModel, dataSet: RDD[LabeledPoint]) = {
    model.clearThreshold()
    dataSet.map(f => (f.label, model.predict(f.features)))
  }

  def binaryLogisticRegressionPredict(model: LogisticRegressionModel, dataSet: RDD[LabeledPoint]) = {
    model.clearThreshold()
    dataSet.map(f => (f.label, model.predict(f.features)))
  }

  //  private
  def linearPredict(model: GeneralizedLinearModel, dataSet: RDD[LabeledPoint]) =
    dataSet.map(f => (f.label, model.predict(f.features)))

  def naiveBayesPredict(model: NaiveBayesModel, rdd: RDD[LabeledPoint]) =
    rdd.map(f => (f.label, model.predict(f.features)))

  private def extractDataAndIndexesStr(
    train: DataFrame,
    test: DataFrame,
    ids: Seq[String] = Seq("id"),
    target: Seq[String] = Seq("target"),
    includes: Seq[String] = Seq.empty,
    excludes: Seq[String] = Seq.empty
  ): (Dataset, Dataset, scala.Seq[String], scala.Seq[String]) = {
    val includedFields = if (includes.isEmpty) {
      train.columnNames()
    } else ids ++ target ++ includes
    val filteredTrainDataSet = train sliceByName (includedFields, excludes)
    val testIncludes =
      if (includes.isEmpty) test.columnNames() else ids ++ includes
    val filteredTestDataSet = test.sliceByName(testIncludes, excludes)
    (filteredTrainDataSet, filteredTestDataSet, target, ids)
  }

  private def buildTestAndTrainDataSet(filteredTrainDataSet: Dataset,
                                       filteredTestDataSet: Dataset,
                                       targetName: Seq[String],
                                       idName: Seq[String],
                                       validationPercentage: Double = 0.3d) = {
    val filteredTrainDataSetCached = filteredTrainDataSet.cache
    val (train, test, _) = createLabeledPointFromRDD(
      filteredTrainDataSetCached,
      filteredTestDataSet,
      targetName,
      idName
    )
    val (trainDataSet, validationDataSet) =
      splitRddInTestValidation(train, (1D - validationPercentage))

    (trainDataSet.cache, filteredTrainDataSetCached, validationDataSet.cache, test)
  }

  //  private
  def splitRddInTestValidation[T](dataSet: RDD[T], train: Double = 0.7): (RDD[T], RDD[T]) = {
    require(train < 1d)
    require(train > 0)
    val split = dataSet.randomSplit(Array(train, 1 - train))
    split match {
      case Array(a: RDD[T], b: RDD[T]) => (a, b)
      case _ =>
        throw new InvalidDataException(s"Unexpected split data result ")
    }
  }

  /**/
  protected def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                  algorithm: Algorithm,
                                  iterations: Int) = {
    algorithm match {
      case BinarySupportVectorMachines => {
        binarySVMModel(trainDataSet, iterations)
      }
      case BinaryLogisticRegressionSGD => {
        binaryLogisticRegressionModelSGD(trainDataSet, iterations)
      }
      case BinaryLogisticRegressionBFGS => {
        binaryLogisticRegressionModel(trainDataSet, iterations)
      }
      case ArtificialNeuralNetwork =>
        neuralNetwork(trainDataSet, iterations)
      case NaiveBayesClassifier => naiveBayesModel(trainDataSet, iterations)
      case LinearLeastSquares =>
        linearLeastSquaresModel(trainDataSet, iterations)
    }
  }

  def binaryLogisticRegressionModelSGD(dataSet: RDD[LabeledPoint], iterations: Int) = {
    val regression = new LogisticRegressionWithSGD()
    regression.optimizer.setNumIterations(iterations)
    //    regression.optimizer.setRegParam(0.1d)
    val model = regression.run(dataSet)
    // model.clearThreshold()
    TrainedData[LogisticRegressionModel](
      model,
      BinaryLogisticRegressionSGD,
      iterations
    )
  }

  def binaryLogisticRegressionModel(dataSet: RDD[LabeledPoint], iterations: Int) = {
    val regression = new LogisticRegressionWithLBFGS()
    regression.optimizer.setNumIterations(iterations)
    val model = regression.run(dataSet)
    // model.clearThreshold()
    TrainedData[LogisticRegressionModel](
      model,
      BinaryLogisticRegressionBFGS,
      iterations
    )
  }

  //  private
  def binarySVMModel(dataSet: RDD[LabeledPoint], iterations: Int) = {
    val model = SVMWithSGD.train(dataSet, iterations)
    model.clearThreshold()
    TrainedData[SVMModel](model, BinarySupportVectorMachines, iterations)
  }

  //  private
  def naiveBayesModel(dataSet: RDD[LabeledPoint], iterations: Int) = {
    val model = NaiveBayes.train(dataSet) //missing lambda
    TrainedData[NaiveBayesModel](model, NaiveBayesClassifier, iterations)
  }

  //private
  def linearLeastSquaresModel(trainDataSet: RDD[LabeledPoint], iterations: Int) = {
    val model = LassoWithSGD.train(trainDataSet, iterations)
    TrainedData[RegressionModel](model, LinearLeastSquares, iterations)
  }

  private def neuralNetwork(trainDataSet: RDD[LabeledPoint], iterations: Int) = {

    val annModel =
      org.apache.spark.mllib.classification.ANNClassifier.train(trainDataSet)
    TrainedData[ANNClassifierModel](
      annModel,
      ArtificialNeuralNetwork,
      iterations
    )
  }

  def predictDecisionTree(train: Dataset,
                          test: Dataset,
                          targetIndexes: Seq[String] = Seq("id"),
                          testTargetIndexes: Seq[String] = Seq("target"),
                          validationPercentage: Double = 0.3d,
                          impurity: String = "gini",
                          maxDepth: Int = 5,
                          maxBins: Option[Int] = None) = {
    val (trainLabel, fullTestLabel, summarizedColumns) =
      createLabeledPointFromRDD(train, test, targetIndexes, testTargetIndexes)
    val (trainDataSet, validationDataSet) =
      splitRddInTestValidation(trainLabel, (1D - validationPercentage))
    val trans = summarizedColumns.map(f => (-1 + f._1 -> f._2._1))

    val categoricalFeatures = trans.collectAsMap.filter(_._2 >= 2)
    val bins = maxBins.getOrElse(
      categoricalFeatures.headOption.map(_ => categoricalFeatures.values.max + 1).getOrElse(100)
    )
    val dtreeModel = TrainedData(
      DecisionTree.trainClassifier(
        trainDataSet.map(_._2).cache,
        2,
        categoricalFeatures.toMap,
        impurity,
        maxDepth,
        bins
      ),
      DecisionTreeAlg,
      maxDepth
    )

    val testPrediction = decisionTreePredict(
      dtreeModel.model.asInstanceOf[DecisionTreeModel],
      fullTestLabel.values.cache()
    )
    val validationPrediction = decisionTreePredict(
      dtreeModel.model.asInstanceOf[DecisionTreeModel],
      validationDataSet.values.cache
    )
    datasetKeyJoin(
      fullTestLabel,
      testPrediction,
      validationDataSet,
      validationPrediction,
      trainDataSet,
      dtreeModel
    )

  }

  def predictMultiple(trainSchema: DataFrame,
                      testSchema: DataFrame,
                      ids: Seq[Int] = Seq(0),
                      target: Seq[Int] = Seq(1),
                      includes: Seq[Int] = Seq.empty,
                      excludes: Seq[Int] = Seq.empty,
                      validationPercentage: Double = 0.3d,
                      algorithm: List[Algorithm],
                      iterations: Int = 100) = {
    analyzeRequirements(includes, excludes, target, ids)
    val (
    filteredTrainDataSet,
    filteredTestDataSet,
    targetIndexes,
    testTargetIndexes
    ) = extractDataAndIndexes(
      trainSchema,
      testSchema,
      ids,
      target,
      includes,
      excludes
    )
    val (trainDataSet, _, validationDataSet, testDataSet) =
      buildTestAndTrainDataSet(
        filteredTrainDataSet,
        filteredTestDataSet,
        targetIndexes,
        testTargetIndexes,
        validationPercentage
      )

    val targetName = mapIntIdsToString(trainSchema, target)
    val idName = mapIntIdsToString(trainSchema, ids)
    val result = algorithm.map { alg =>
      alg match {
        case BinarySupportVectorMachines =>
          predictSVM(
            filteredTrainDataSet,
            filteredTestDataSet,
            targetName,
            idName,
            validationPercentage,
            iterations
          )
        case DecisionTreeAlg =>
          predictDecisionTree(
            filteredTrainDataSet,
            filteredTestDataSet,
            targetName,
            idName,
            validationPercentage
          )
        case _ =>
          val (
          validationPrediction,
          model,
          testDataSet,
          validationDataSet,
          trainDataSetCached,
          testPredict
          ) =
            predictGeneric(
              filteredTrainDataSet,
              filteredTestDataSet,
              targetName,
              idName,
              validationPercentage,
              alg,
              iterations
            )
          datasetKeyJoin(
            testDataSet,
            testPredict,
            validationDataSet,
            validationPrediction,
            trainDataSetCached,
            model
          )
      }
    }

    val multiplePredictionValidation = result.map { f =>
      f.model.algorithm -> f.validationPrediction
    }.toMap
    val multiplePredictionTest = result.map { f =>
      f.model.algorithm -> f.testPredictionId
    }.toMap
    val models = result.map(_.model)
    result.map { f =>
      f
    }
    MultiplePrediction(
      multiplePredictionValidation,
      validationDataSet,
      trainDataSet,
      multiplePredictionTest,
      testDataSet,
      models
    )
  }

  def predictMultipleString(trainSchema: DataFrame,
                            testSchema: DataFrame,
                            ids: Seq[String] = Seq("id"),
                            target: Seq[String] = Seq.empty[String],
                            includes: Seq[String] = Seq.empty,
                            excludes: Seq[String] = Seq.empty,
                            validationPercentage: Double = 0.3d,
                            algorithm: List[Algorithm],
                            iterations: Int = 100) = {
    analyzeRequirements(includes, excludes, target, ids)
    val (
    filteredTrainDataSet,
    filteredTestDataSet,
    targetIndexes,
    testTargetIndexes
    ) = extractDataAndIndexesStr(
      trainSchema,
      testSchema,
      ids,
      target,
      includes,
      excludes
    )
    val (trainDataSet, _, validationDataSet, testDataSet) =
      buildTestAndTrainDataSet(
        filteredTrainDataSet,
        filteredTestDataSet,
        targetIndexes,
        testTargetIndexes,
        validationPercentage
      )

    val result = algorithm.map { alg =>
      alg match {
        case BinarySupportVectorMachines => {
          predictSVM(
            filteredTrainDataSet,
            filteredTestDataSet,
            target,
            ids,
            validationPercentage,
            iterations
          )
        }
        case DecisionTreeAlg => {

          predictDecisionTree(
            filteredTrainDataSet,
            filteredTestDataSet,
            target,
            ids,
            validationPercentage
          )
        }
        case _ =>
          val (
          validationPrediction,
          model,
          testDataSet,
          validationDataSet,
          trainDataSetCached,
          testPredict
          ) =
            predictGeneric(
              filteredTrainDataSet,
              filteredTestDataSet,
              target,
              ids,
              validationPercentage,
              alg,
              iterations
            )
          datasetKeyJoin(
            testDataSet,
            testPredict,
            validationDataSet,
            validationPrediction,
            trainDataSetCached,
            model
          )
      }
    }

    val multiplePredictionValidation = result.map { f =>
      f.model.algorithm -> f.validationPrediction
    }.toMap
    val multiplePredictionTest = result.map { f =>
      f.model.algorithm -> f.testPredictionId
    }.toMap
    val models = result.map(_.model)
    result.map { f =>
      f
    }
    MultiplePrediction(
      multiplePredictionValidation,
      validationDataSet,
      trainDataSet,
      multiplePredictionTest,
      testDataSet,
      models
    )

  }

  private def analyzeRequirements(includes: Seq[_],
                                  excludes: Seq[_],
                                  target: Seq[_],
                                  ids: Seq[_]) = {
    require(
      !includes.isEmpty || !excludes.isEmpty,
      "Includes or excludes can't be empty"
    )
    require(!target.isEmpty, "Target field can't be empty")
    require(!ids.isEmpty, "ids field can't be empty")
  }

  private def extractDataAndIndexes(
    train: DataFrame,
    test: DataFrame,
    ids: Seq[Int] = Seq(0),
    target: Seq[Int] = Seq(1),
    includes: Seq[Int] = Seq.empty,
    excludes: Seq[Int] = Seq.empty
  ): (Dataset, Dataset, scala.Seq[String], scala.Seq[String]) = {
    val targetIndexes = mapIntIdsToString(train, target)
    val idsIndexes = mapIntIdsToString(train, ids)
    val includesIndexes = mapIntIdsToString(train, includes)
    val excludesIndexes = mapIntIdsToString(train, excludes)
    extractDataAndIndexesStr(
      train,
      test,
      idsIndexes,
      targetIndexes,
      includesIndexes,
      excludesIndexes
    )
  }

  private def train(trainDataSetAfterValidationSplit: RDD[LabeledPoint],
                    trainDataSetCached: Dataset,
                    targetIndexes: Seq[String],
                    algorithm: Algorithm,
                    iterations: Int) = {
    val targetIndex = targetIndexes.map(trainDataSetCached.columnIndexOf(_))
    val selectedAlgorithm =
      determineAlgorithm(trainDataSetCached, targetIndex.head, algorithm)
    trainForAlgorithm(
      trainDataSetAfterValidationSplit,
      selectedAlgorithm,
      iterations
    )
  }

  //  private
  def determineAlgorithm(dataSet: DataFrame, targetIndex: Int, algorithm: Algorithm) = {
    algorithm match {
      case ToBeDetermined =>
        val targetDataSet = dataSet.rdd.map(f => f(targetIndex)).distinct()
        targetDataSet.collect().size match {
          case 0 | 1 =>
            throw new InvalidDataException(
              "Predicted column should have more than 1 value."
            )
          case 2 => BinarySupportVectorMachines
          case _ =>
            dataSet.schema.fields(targetIndex).dataType match {
              case StringType => NaiveBayesClassifier
              case _ => LinearLeastSquares
            }
        }
      case _ => algorithm
    }
  }

  def decisionTreePredict(model: DecisionTreeModel, label: LabeledPoint) =
    model.predict(label.features)

  def predictInt(train: DataFrame,
                 test: DataFrame,
                 ids: Seq[Int] = Seq(0),
                 target: Seq[Int] = Seq(1),
                 includes: Seq[Int] = Seq.empty,
                 excludes: Seq[Int] = Seq.empty,
                 validationPercentage: Double = 0.3d,
                 algorithm: Algorithm,
                 iterations: Int = 100) = {
    analyzeRequirements(includes, excludes, target, ids)
    val targetStr = mapIntIdsToString(train, target)
    val idsStr = mapIntIdsToString(train, ids)
    val includesStr = mapIntIdsToString(train, includes)
    val excludesStr = mapIntIdsToString(train, excludes)
    predict(
      train,
      test,
      idsStr,
      targetStr,
      includesStr,
      excludesStr,
      validationPercentage,
      algorithm,
      iterations
    )
  }

  def predict(train: DataFrame,
              test: DataFrame,
              ids: Seq[String] = Seq("id"),
              target: Seq[String] = Seq.empty[String],
              includes: Seq[String] = Seq.empty,
              excludes: Seq[String] = Seq.empty,
              validationPercentage: Double = 0.3d,
              algorithm: Algorithm,
              iterations: Int = 100) = {
    analyzeRequirements(includes, excludes, target, ids)
    val start = System.currentTimeMillis()
    val (filteredTrainDataSet, filteredTestDataSet, targetName, idName) =
      extractDataAndIndexesStr(train, test, ids, target, includes, excludes)
    val result = algorithm match {
      case BinarySupportVectorMachines =>
        predictSVM(
          filteredTrainDataSet,
          filteredTestDataSet,
          targetName,
          idName,
          validationPercentage,
          iterations
        )
      case DecisionTreeAlg =>
        predictDecisionTree(
          filteredTrainDataSet,
          filteredTestDataSet,
          targetName,
          idName,
          validationPercentage
        )
      case _ => {
        val (
        validationPrediction,
        model,
        testDataSet,
        validationDataSet,
        trainDataSetCached,
        testPredict
        ) =
          predictGeneric(
            filteredTrainDataSet,
            filteredTestDataSet,
            targetName,
            idName,
            validationPercentage,
            algorithm,
            iterations
          )
        datasetKeyJoin(
          testDataSet,
          testPredict,
          validationDataSet,
          validationPrediction,
          trainDataSetCached,
          model
        )
      }
    }
    println(s"spent time ${(System.currentTimeMillis() - start) / 1000}")
    result
  }

  def predictSVM(engineeredTrainingFeatures: DataFrame,
                 engineeredTestFeatures: DataFrame,
                 targetIndexes: Seq[String],
                 testTargetIndexes: Seq[String],
                 validationPercentage: Double = 0.3d,
                 iterations: Int) = {
    val (
    validationPredict,
    model,
    testDataSet,
    validationDataSet,
    trainDataSetCached,
    testPredict
    ) = predictGeneric(
      engineeredTrainingFeatures,
      engineeredTestFeatures,
      targetIndexes,
      testTargetIndexes,
      validationPercentage,
      BinarySupportVectorMachines,
      iterations
    )

    val validationLabel = convDoubleDoubleToLabeledPoint(validationPredict)
    val testLabel = convDoubleDoubleToLabeledPoint(testPredict)
    val logisticModel =
      binaryLogisticRegressionModel(validationLabel.values, iterations)
    val validationPrediction = binaryLogisticRegressionPredict(
      logisticModel.model.asInstanceOf[LogisticRegressionModel],
      validationLabel.values
    )
    val logisticPrediction = binaryLogisticRegressionPredict(
      logisticModel.model.asInstanceOf[LogisticRegressionModel],
      testLabel.values.cache
    )
    datasetKeyJoin(
      testDataSet,
      logisticPrediction,
      validationDataSet,
      validationPrediction,
      trainDataSetCached,
      model
    )
  }

  def datasetKeyJoin(testDataSet: RDD[((Double, Any), LabeledPoint)],
                     testPrediction: RDD[(Double, Double)],
                     validationDataSet: RDD[((Double, Any), LabeledPoint)],
                     validationPrediction: RDD[(Double, Double)],
                     trainDataSetCached: RDD[((Double, Any), LabeledPoint)],
                     model: TrainedData[scala.Serializable]): Prediction = {
    val testPredictionId = datasetKeyJoin(testDataSet.keys, testPrediction)
    val validationPredictionId =
      datasetKeyJoin(validationDataSet.keys, validationPrediction)
    val trainPredictionId =
      datasetKeyJoinLabel(trainDataSetCached.keys, trainDataSetCached.values)

    Prediction(
      validationPrediction,
      model,
      testDataSet,
      validationPredictionId,
      trainPredictionId,
      testPredictionId
    )
  }

  def datasetKeyJoin(testKeys: RDD[(Double, Any)], testPredict: RDD[(Double, Double)]) = {
    import org.apache.spark.rdd.PairRDDFunctions
    testKeys.join(testPredict).values
  }

  def datasetKeyJoinLabel(testKeys: RDD[(Double, Any)], values: RDD[LabeledPoint]) = {
    testKeys.join(values.map(labelPoint => (labelPoint.label, labelPoint))).values
  }

  def predictGeneric(
    filteredTrainDataSet: Dataset,
    filteredTestDataSet: Dataset,
    targetName: Seq[String],
    idName: Seq[String],
    validationPercentage: Double = 0.3d,
    algorithm: Algorithm,
    iterations: Int
  ): (RDD[(Double, Double)],
      TypeMixin.TrainedData[scala.Serializable],
      RDD[((Double, Any), LabeledPoint)],
      RDD[((Double, Any), LabeledPoint)],
      RDD[((Double, Any), LabeledPoint)],
      RDD[(Double, Double)]) = {
    val (trainDataSetCached, _, validationDataSet, testDataSet) =
      buildTestAndTrainDataSet(
        filteredTrainDataSet,
        filteredTestDataSet,
        targetName,
        idName,
        validationPercentage
      )
    val model = train(
      trainDataSetCached.values,
      filteredTrainDataSet,
      targetName,
      algorithm,
      iterations
    )
    val validationPredict = executePredict(model, validationDataSet.values)
    val testPredict = executePredict(model, testDataSet.values.cache)

    (validationPredict, model, testDataSet, validationDataSet, trainDataSetCached, testPredict)
  }

  def decisionTreePredict(model: DecisionTreeModel, dataSet: RDD[LabeledPoint]) =
    dataSet.map(f => (f.label, model.predict(f.features)))

  def binaryStatistics(scoreAndLabels: RDD[(Double, Double)]) =
    new BinaryClassificationMetrics(scoreAndLabels)

  def linearLeastStatistics(valuesAndPredict: RDD[(Double, Double)]) = {
    val MSE = stats(valuesAndPredict.map {
      case (v, p) => math.pow((v - p), 2)
    }).mean
    println("training Mean Squared Error = " + MSE)
    MSE
  }

  //  private
  def stats(rdd: RDD[Double]): StatCounter =
    rdd.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b))

  def linearPredict(model: LassoModel, id: Any, label: LabeledPoint) =
    (id, model.predict(label.features))

  //  private
  def linearPredict(model: LassoModel, parsedData: RDD[(Any, LabeledPoint)]) =
    parsedData.map {
      case (ids, point) =>
        val prediction = model.predict(point.features)
        (ids, prediction)
    }

  private def convDoubleDoubleToLabeledPoint(
    dataSet: RDD[(Double, Double)]
  ): RDD[(Double, LabeledPoint)] = dataSet.map { f =>
    (f._1, LabeledPoint(f._1, org.apache.spark.mllib.linalg.Vectors.fromML(Vectors.dense(f._2))))
  }

  private def evolutivePredictObjectSplit(train: Dataset,
                                          test: Dataset,
                                          ids: Seq[String],
                                          target: Seq[String]) = {

    val (trainDataSet, validationDataSet) = splitRddInTestValidation(
      train.toDataFrame.rdd
    )
    val cachedTrain = train.sqlContext
      .createDataFrame(trainDataSet, train.schema)
      .persist(StorageLevel.MEMORY_ONLY_SER)
    val cachedValidation = train.sqlContext
      .createDataFrame(validationDataSet, train.schema)
      .persist(StorageLevel.MEMORY_ONLY_SER)
    val cachedTest = test.persist(StorageLevel.MEMORY_ONLY_SER)
    (cachedTrain, cachedValidation, cachedTest)
  }

  @tailrec
  private def evolutivePrediction(
    train: Dataset,
    validation: Dataset,
    test: Dataset,
    ids: Seq[String],
    target: Seq[String],
    validationPercentage: Double,
    algorithm: Algorithm,
    validationMethod: ValidationMethod,
    steps: Seq[Step],
    iterations: Int,
    columnList: List[String],
    modelList: Array[(Models, Double, Array[String])],
    unionDataset: Dataset
  ): Array[(Models, Double, Array[String])] = {

    if (steps.isEmpty) modelList //.map(f => (f._1,f._2,f._3.map(_+1)))
    else {
      val step = steps.head
      val columns = columnList.headOption
        .map(_ => columnList)
        .getOrElse(
          train
            .columnNames()
            .filter(
              columnName => !ids.contains(columnName) && !target.contains(columnName)
            )
        )
        .distinct
        .take(step.nextStepAmount)
      val selectedColumns = columns.combinations(step.groupAmount).toArray.par

      //    to parallelize more than the default
      //      val parallelism = Math.max (Math.min(16*ForkJoinTasks.defaultForkJoinPool.getParallelism,
      //        train.sqlContext.sparkContext.getExecutorMemoryStatus.size * 16), ForkJoinTasks.defaultForkJoinPool.getParallelism)
      //      selectedColumns.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

      println(s"Predictions this step: ${selectedColumns.size}")
      println(s"Remaining steps: ${steps.size - 1}")
      println(
        s"Selected Column Names ${selectedColumns.flatten.mkString(",")}"
      )
      println(s"Predictions this step: ${selectedColumns.size}")
      //      println(s"Models processed in parallel: ${parallelism}")

      val trainDataSets = selectedColumns.map { includes =>
        val includesColIndex =
          includes.map(columnName => unionDataset.columnIndexOf(columnName))
        val filteredSummarizedColumns = unionDataset.summarizedColumns
          .filter(n => includesColIndex.contains(n._1))
          .sortBy(_._1)
          .zipWithIndex
          .map(n => (n._2.toInt, n._1._2))
        val columnsSize = filteredSummarizedColumns.map(_._2._1).sum().toInt
        val trainDataSetCached = createLabeledPointFromRDD(
          train.sliceByName(includes = includes ++ target ++ ids),
          target,
          ids,
          filteredSummarizedColumns,
          DataSetType.Train,
          columnsSize
        ).values
          .setName(s"trainingDataset-$step-Columns($includes)")
          .persist(StorageLevel.MEMORY_ONLY_SER)
        val validationDataSetCached = createLabeledPointFromRDD(
          validation.sliceByName(includes = includes ++ target ++ ids),
          target,
          ids,
          filteredSummarizedColumns,
          DataSetType.Train,
          columnsSize
        ).values
        val prediction = partialPredict(
          trainDataSetCached,
          validationDataSetCached,
          validationMethod,
          iterations,
          algorithm,
          includes.toArray
        )
        validationDataSetCached.unpersist(false)
        trainDataSetCached.unpersist(false)
        prediction
      }
      evolutivePrediction(
        train,
        validation,
        test,
        ids,
        target,
        validationPercentage,
        algorithm,
        validationMethod,
        steps.tail,
        iterations,
        take(trainDataSets.toArray, validationMethod),
        modelList ++ trainDataSets,
        unionDataset
      )
    }
  }
}
