package eleflow.uberdata.data.stat

import eleflow.uberdata.core.data.{Dataset, DataTransformer}
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.stat.{Statistics => SparkStatistics}

/**
  * Created by dirceu on 11/12/14.
  */
object Statistics {


  def logLoss(validationPrediction: RDD[(Double, Double)]) = logarithmicLoss(validationPrediction)

  def logarithmicLoss(validationPrediction: RDD[(Double, Double)]) = -(validationPrediction.map { case (act, pred) =>
    val epsilon = 1e-15
    val pred2 = Math.min(1 - epsilon, Math.max(epsilon, pred))
    act * Math.log(pred2) + (1 - act) * Math.log(1 - pred2)
  }.sum / validationPrediction.count)

  def correlation(rdd: RDD[LabeledPoint]): Matrix = SparkStatistics.corr(rdd.map(_.features))


  def correlation(rdd: Dataset, numberOfColumns: Int = 20): Seq[(Double, String)] = {
    val matrix = correlation(rdd.toLabeledPoint)
    val correlat = correlation(matrix, rdd)
    correlat.map {
      case (columnA, columnB, corr) =>
        (corr, s"$columnA | $columnB")
    }.seq.take(numberOfColumns)
  }

  private def correlation(matrix: Matrix, rdd: Dataset): IndexedSeq[((String, String), (String, String), Double)] = {
    val cols = matrix.numCols
    val rows = matrix.numRows
    val array = matrix.toArray
    (0 until rows).flatMap { row =>
      (row + 1 until cols).map { col =>
        (rdd.summarizedColumnsIndex.getOrElse(col, ("Unknow", "Unknow")),
          rdd.summarizedColumnsIndex.getOrElse(row, ("Unknow", "Unknow")), array(col + (row * cols)))
      }
    }.filter {
      case (col, row, corr) =>
        col._1 != row._1 && !corr.isNaN
    }.sortBy(f => -Math.abs(f._3))
  }

  //Metodo pra computar a correlacao dos valores de cada coluna com a coluna de target
  def targetCorrelation(rdd: RDD[LabeledPoint]): Seq[(Double, Int)] = {
    val targetVectors =
      rdd.map(f => Vectors.dense(DataTransformer.toDouble(f.label), f.features.toArray: _*))
    val correlated = SparkStatistics.corr(targetVectors)
    val cols = correlated.numCols

    val array = correlated.toArray
    (1 until cols).map { index =>
      (array(index).abs, index - 1)
    }.filter(f => !f._1.isNaN).sortBy(-_._1)
  }

  def columnAndTargetCorrelation(dataset: Dataset, numberOfColumns: Int = 20, ids: Seq[String] = Seq("id")) = (correlation
  (dataset, numberOfColumns),
    targetCorrelation(dataset, numberOfColumns, ids))

  def targetCorrelation(rdd: Dataset, numberOfColumns: Int = 20, ids: Seq[String] = Seq("id")): Seq[(Double, Any)] = {
    val correlation = targetCorrelation(rdd.sliceByName(excludes = ids).toLabeledPoint).take(numberOfColumns)

    correlation.map {
      case (corr, index) =>
        (corr, rdd.summarizedColumnsIndex.getOrElse(index + 1 + ids.size, "Unknow"))
    }.seq
  }

  def targetCorrelation(rdd: RDD[LabeledPoint], numberOfColumns: Int): Seq[(Double, Int)] =
    targetCorrelation(rdd).take(numberOfColumns)

  def correlationLabeledPoint(train: RDD[LabeledPoint], validation: RDD[LabeledPoint],
                              test: RDD[LabeledPoint], selectColumns: Either[Int, Double]):
  (RDD[LabeledPoint], RDD[LabeledPoint], RDD[LabeledPoint], Seq[Int]) = {
    val target = targetCorrelation(train)
    selectColumns match {
      case Left(l) =>
        val correlatedColumns = target.take(l).map(_._2)
        (train.map(filterCorrelatedLabeledPointValues(_, correlatedColumns)),
          validation.map(filterCorrelatedLabeledPointValues(_, correlatedColumns)),
          test.map(filterCorrelatedLabeledPointValues(_, correlatedColumns)),
          correlatedColumns)
      case Right(r) =>
        println(target.mkString)
        val correlatedColumns = target.filter(_._1 > r).map(_._2)
        (train.map(filterCorrelatedLabeledPointValues(_, correlatedColumns)),
          validation.map(filterCorrelatedLabeledPointValues(_, correlatedColumns)),
          test.map(filterCorrelatedLabeledPointValues(_, correlatedColumns)),
          correlatedColumns)
    }
  }

  private def filterCorrelatedLabeledPointValues(f: LabeledPoint, correlatedColumns: Seq[Int]) =
    (LabeledPoint(f.label, Vectors.dense(f.features.toArray.zipWithIndex.filter {
      case (_, index) => correlatedColumns.contains(index)
    }.map(_._1))))
}
