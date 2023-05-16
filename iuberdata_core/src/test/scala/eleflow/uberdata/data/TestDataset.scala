package eleflow.uberdata.data

import eleflow.uberdata.core.conf.SparkNotebookConfig
import eleflow.uberdata.core.data.UberDataset._
import eleflow.uberdata.core.data.{DataTransformer, UberDataset}
import eleflow.uberdata.core.enums.{DataSetType, DateSplitType}
import eleflow.uberdata.core.util.ClusterSettings
import eleflow.uberdata.util.DateUtil
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.nio.file.{FileSystems, Files}
import java.util.Objects

/**
  * Created by caio.martins on 19/10/16.
  */
class TestDataset extends AnyFlatSpec with should.Matchers with BeforeAndAfterWithContext  {

//  val  model: TrainValidationSplitModel

  it should "Correct handle date dayofaweek values" in {
    val dataSet = UberDataset(context, s"${defaultFilePath}HandleDataTransformer.csv")
    val ndataSet  = dataSet.formatDateValues(4,DateSplitType.DayOfAWeek)

    val results = DataTransformer.createLabeledPointFromRDD(ndataSet,Seq("int"), Seq("id"), DataSetType.Train).collect()
    assert(results(0)._1._2 == 1)
    assert(results(1)._1._2 == 2)
    assert(results(2)._1._2 == 3)

//    assert(results(0)._2.features.toArray.deep == Array(0.0, 1.0, 10.5, 4.0).deep)
//    assert(results(1)._2.features.toArray.deep == Array(1.0, 0.0, 0.1, 6.0).deep)
//    assert(results(2)._2.features.toArray.deep == Array(1.0, 0.0, 10.0, 6.0).deep)
    assert(Objects.deepEquals(results(0)._2.features.toArray, Array(0.0, 1.0, 10.5, 4.0)))
    assert(Objects.deepEquals(results(1)._2.features.toArray, Array(1.0, 0.0, 0.1, 6.0)))
    assert(Objects.deepEquals(results(2)._2.features.toArray, Array(1.0, 0.0, 10.0, 6.0)))
  }

  it should "Correct handle date dayofaweek and period values" in {
    //context.sparkContext.clearJars()
    DateUtil.applyDateFormat("YYMMddHH")
    val fileDataset = UberDataset(context, s"${defaultFilePath}DayOfAWeekDataTransformer.csv")
    fileDataset.applyColumnTypes(Seq(LongType, LongType, StringType, DecimalType(
      ClusterSettings.defaultDecimalPrecision,ClusterSettings.defaultDecimalScale), TimestampType))

    val dataset = FileDatasetToDataset(fileDataset)

    val datasetWithDate : Array[Row] = dataset.formatDateValues(4, DateSplitType.DayOfAWeek | DateSplitType.Period).collect()
    assert(datasetWithDate(0)(4) == 4)
    assert(datasetWithDate(0)(5) == 3)
    assert(datasetWithDate(1)(4) == 5)
    assert(datasetWithDate(1)(5) == 3)
    assert(datasetWithDate(2)(4) == 6)
    assert(datasetWithDate(2)(5) == 3)
    val filePath = FileSystems.getDefault.getPath(SparkNotebookConfig.propertyFolder, SparkNotebookConfig.dateFormatFileName)
    Files.deleteIfExists(filePath)
  }

}