package eleflow.uberdata.data

import eleflow.uberdata.core.ClusterSettings
import eleflow.uberdata.core.data.Dataset._
import eleflow.uberdata.core.data.{DataTransformer, Dataset}
import eleflow.uberdata.core.enums.DataSetType
import eleflow.uberdata.core.util.DateTimeParser
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.types._
import org.scalatest._

/**
 * Created by dirceu on 21/10/14.
 */
class TestDataTransformer extends FunSuite with Matchers with BeforeAndAfterWithContext {
  this: Suite =>


    //"DataTransformer "  should
  test("correctly load rdd with empty columns at the end") {
    val testDataSet = Dataset(context, s"${defaultFilePath}LoadRddDataTransformerTestData.csv")

    val dataset = Dataset(context, s"${defaultFilePath}LoadRddDataTransformerData.csv")

//    context.sqlContext.createDataFrame(dataset.collect)
    val (result,_,summarizedColumns) = DataTransformer.createLabeledPointFromRDD(dataset,testDataSet, "t1","int")
    val all = result.take(3)
    val (_, first) = all.head
    val (_, second) = all.tail.head
    assert(first.label == 3)

    assert(first.features.toArray.deep == Array[Double](0.0, 1.0, 0.0, 0.0, 0.0,10.5, 0.0, 1.0, 0.0).deep)
    assert(second.label == 4)
    assert(second.features.toArray.deep == Array[Double](1.0, 0.0, 0.0, 0.0, 0.0,0.1, 0.0, 0.0, 1.0).deep)
  }

  test("Correct handle date values") {
    val dataSet = Dataset(context, s"${defaultFilePath}HandleDataTransformer.csv", DateTimeParser(0))

    val results = DataTransformer.createLabeledPointFromRDD(dataSet, Seq("id"), Seq(),DataSetType.Test).take(3)

    assert(results(0)._2.features.toArray.deep == Array(5.0, 0.0, 1.0, 10.5, 394299.0).deep)
    assert(results(1)._2.features.toArray.deep == Array(1.0, 1.0, 0.0, 0.1, 394179.0).deep)
    assert(results(2)._2.features.toArray.deep == Array(8.0, 1.0, 0.0, 10.0, 394179.0).deep)

  }

test("create labeledpoint for multiplestrings") {
    val train = Dataset(context, s"${defaultFilePath}MultipleStringsTest.csv").applyColumnTypes(Seq(LongType, StringType,
      StringType, DecimalType(ClusterSettings.defaultDecimalPrecision,ClusterSettings.defaultDecimalScale), StringType,
      StringType,LongType,StringType))

    val labeledPoint = DataTransformer.createLabeledPointFromRDD(train,Seq("int"), Seq(), DataSetType.Test)
    val result = labeledPoint.take(3)
    assert(result.head._1._1 == 5.0)
    assert(result.head._1._2 == 5)
    assert(result.head._2.label == 5.0)
    assert(result.head._2.features.size == 14)
    assert(result.head._2.features.toArray.deep == Array(0.0,1.0,1.0,0.0,10.5,0.0,1.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0).deep)
    assert(result(1)._1._1 == 1.0)
    assert(result(1)._1._2 == 1)
    assert(result(1)._2.label == 1.0)
    assert(result(1)._2.features.toArray.deep == Array(1.0,0.0,1.0,0.0,0.1,1.0,0.0,0.0,1.0,0.0,2.0,0.0,1.0,0.0).deep)
    assert(result(2)._1._1 == 8.0)
    assert(result(2)._1._2 == 8)
    assert(result(2)._2.label == 8.0)
    assert(result(2)._2.features.toArray.deep == Array(0.0,1.0,0.0,1.0,10.0,0.0,0.0,1.0,0.0,1.0,3.0,1.0,0.0,0.0).deep)
  }
}
