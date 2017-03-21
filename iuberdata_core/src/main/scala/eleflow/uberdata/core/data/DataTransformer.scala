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
package eleflow.uberdata.core.data

import java.sql.Timestamp

import Dataset._
import eleflow.uberdata.core.enums.DataSetType
import eleflow.uberdata.core.exception.UnexpectedValueException

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by dirceu on 16/10/14.
  */
object DataTransformer {

  def createLabeledPointFromRDD(
    train: Dataset,
    test: Dataset,
    target: String,
    id: String
  ): (RDD[((Double, Any), LabeledPoint)],
      RDD[((Double, Any), LabeledPoint)],
      RDD[(Int, (Int, (Any) => Int, (Any) => Double))]) =
    createLabeledPointFromRDD(train, test, Seq(target), Seq(id))

  def createLabeledPointFromRDD(
    train: Dataset,
    test: Dataset,
    target: Seq[String],
    id: Seq[String]
  ): (RDD[((Double, Any), LabeledPoint)],
      RDD[((Double, Any), LabeledPoint)],
      RDD[(Int, (Int, (Any) => Int, (Any) => Double))]) = {
    val (newTrain, newTest) = idAndTargetAtHead(train, test, target, id)
    val summarizedColumns = prepareToSummarizeColumns(
      newTrain,
      newTest,
      target,
      id
    ).summarizedColumns

    val columnsSize = summarizedColumns.map(_._2._1).sum().toInt
    (createLabeledPointFromRDD(
       newTrain,
       target,
       id,
       summarizedColumns,
       DataSetType.Train,
       columnsSize
     ),
     createLabeledPointFromRDD(
       newTest,
       id,
       Seq(),
       summarizedColumns,
       DataSetType.Test,
       columnsSize
     ),
     summarizedColumns)
  }

  //TODO
  def idAndTargetAtHead(train: Dataset, test: Dataset, target: Seq[String], id: Seq[String]) = {
    val newTrain =
      if (train.columnIndexOf(target.head) == 1 && train.columnIndexOf(id.head) == 0) {
        train
      } else {
        val trainColumnNames = id ++ target ++ train
            .columnNames()
            .filter(f => !target.contains(f) && !id.contains(f))
        new Dataset(
          train.select(trainColumnNames.head, trainColumnNames.tail: _*)
        )
      }
    val newTest = if (test.columnIndexOf(id.head) == 0) {
      test
    } else {
      val testColumnNames = id ++ test
          .columnNames()
          .filter(f => !target.contains(f) && !id.contains(f))
      new Dataset(test.select(testColumnNames.head, testColumnNames.tail: _*))
    }
    (newTrain, newTest)
  }

  def convertNullValues: Row => Row = { a: Row =>
    Row(a.toSeq.map(f => if (f == null) 0d else f))
  }

  def prepareToSummarizeColumns(train: Dataset,
                                test: Dataset,
                                target: Seq[String],
                                id: Seq[String]) = {
    train.sliceByName(excludes = target).unionAll(test).sliceByName(excludes = id)
  }

  def createLabeledPointFromRDD(
    dataset: Dataset,
    target: Seq[String],
    id: Seq[String],
    summarizedColumns: RDD[(Int, (Int, (Any => Int), (Any => Double)))],
    dataSetType: DataSetType.Types,
    columnsSize: Int
  ): RDD[((Double, Any), LabeledPoint)] = {
    // TODO Break in two methods. One for train another for test
    val fields =
      dataset.dtypes.zipWithIndex.filter(f => !target.contains(f._1._1) && !id.contains(f._1._1))
    val targetFieldType = dataset.dtypes.filter(f => target.contains(f._1))
    val targetIndices = target.map(f => dataset.columnIndexOf(f))
    val idIndices = id.map(f => dataset.columnIndexOf(f))
    val normalizedStrings =
      dataset.sqlContext.sparkContext.broadcast(summarizedColumns.collectAsMap())
    val columnShift = id.size + target.size

    dataset.rdd.zipWithIndex.map {
      case (row, rowIndex) =>
        val norm = normalizedStrings.value
        val normValues = fields.map {
          case (_, index) =>
            val v = norm.get(index - columnShift).map {
              case (id, funcToIndex, funcValue) =>
                (id, funcToIndex(row(index)), funcValue(row(index)))
            }
            v.getOrElse(
              throw new UnexpectedValueException(
                s"Unexpected String Value exception ${norm.get(index)}$index, ${row(index)}"
              )
            )

        }

        val (_, indexes, values) = normValues.tail
          .scanLeft(normValues.head)(
            (b, a) => (b._1 + a._1, b._1 + a._2, a._3)
          )
          .filter(_._3 != 0)
          .unzip3
        val targetIndex = if (targetIndices.isEmpty) 0 else targetIndices.head
        val rowIndexD =
          if (targetFieldType.isEmpty || targetFieldType.head._2 == "StringType") {
            rowIndex.toDouble + 1
          } else {
            toDouble(row(targetIndex))
          }

        dataSetType match {
          case DataSetType.Test =>
            ((rowIndexD, row(targetIndex)),
             LabeledPoint(
               rowIndexD,
               org.apache.spark.mllib.linalg.Vectors.fromML(Vectors.sparse(columnsSize, indexes.toArray, values.toArray))
             ))
          case DataSetType.Train =>
            val idIndex = if (idIndices.isEmpty) 0 else idIndices.head
            ((rowIndexD, row(idIndex)),
             LabeledPoint(
               rowIndexD,
               org.apache.spark.mllib.linalg.Vectors.fromML(Vectors.sparse(columnsSize, indexes.toArray, values.toArray))
             ))
        }
    }
  }

  def toFloat(toConvert: Any): Float = {
    toConvert match {
      case v: Int => v.toFloat
      case v: Long => v.toFloat
      case v: BigDecimal => v.toFloat
      case v: java.math.BigDecimal => v.floatValue()
      case v: Double => v.toFloat
      case v: Float => v
      case v: Timestamp => (v.getTime / 3600000).toFloat
      case v: String => v.toFloat
      case v: Byte => v.toFloat
      case v: Short => v.toFloat
      case v: Boolean =>
        v match {
          case true => 1f
          case false => 0f
        }
      case _ => throw new Exception(toConvert.toString)
    }
  }

  def toDouble(toConvert: Any): Double = {
    toConvert match {
      case v: Int => v.toDouble
      case v: Long => v.toDouble
      case v: BigDecimal => v.toDouble
      case v: java.math.BigDecimal => v.doubleValue()
      case v: Double => v
      case v: Timestamp => (v.getTime / 3600000).toDouble
      case v: String => v.toDouble
      case v: Byte => v.toDouble
      case v: Boolean =>
        v match {
          case true => 1d
          case false => 0d
        }
      case _ => throw new Exception(toConvert.toString)
    }
  }

  def createLabeledPointFromRDD(
    dataset: Dataset,
    target: Seq[String],
    id: Seq[String],
    datasetType: DataSetType.Types
  ): RDD[((Double, Any), LabeledPoint)] = {
    createLabeledPointFromRDD(
      dataset,
      target,
      id,
      dataset.sliceByName(excludes = id ++ target).summarizedColumns,
      datasetType,
      dataset.columnsSize.toInt - target.size - id.size
    )
  }

  def mapIntIdsToString(rdd: DataFrame, columns: Seq[Int]): Seq[String] =
    rdd.schema.fields.zipWithIndex.filter {
      case (field, index) => columns.contains(index)
    }.map(_._1.name)

  def extractColumnsFromLP(columns: Array[(Double, Int)],
                           train: RDD[LabeledPoint],
                           validation: RDD[LabeledPoint],
                           test: RDD[((Double, Any), LabeledPoint)])
    : (RDD[LabeledPoint], RDD[LabeledPoint], RDD[((Double, Any), LabeledPoint)]) = {
    val (_, columnsIndex) = columns.unzip
    val trainlp = extractColumns(columnsIndex, train)
    val validationlp = extractColumns(columnsIndex, validation)
    val testlp = test.map {
      case (ids, lp) => (ids, extractLP(columnsIndex)(lp))
    }
    (trainlp, validationlp, testlp)
  }

  def extractColumns(columnsIndex: mutable.IndexedSeq[Int], train: RDD[LabeledPoint]) = {
    train.map(extractLP(columnsIndex)(_))
  }

  def extractLP(columnsIndex: mutable.IndexedSeq[Int]) = { lp: LabeledPoint =>
    val featureArray = lp.features.toArray
    val values = columnsIndex.map { index =>
      featureArray(index)
    }.zipWithIndex.filter(_._1 != 0d).toArray
    val (attributes, indexes) = values.unzip
    val vector =
      Vectors.sparse(columnsIndex.size, indexes.toArray, attributes.toArray)
    LabeledPoint(lp.label, org.apache.spark.mllib.linalg.Vectors.fromML(vector))
  }
}
