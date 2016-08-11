package eleflow.uberdata

import eleflow.uberdata.core.data.Dataset
import Dataset._
import eleflow.uberdata.enums.SupportedAlgorithm
import eleflow.uberdata.core.enums.DateSplitType._
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers, Suite}
import eleflow.uberdata.enums.SupportedAlgorithm._
import org.apache.spark.ml.ArimaModel
import org.apache.spark.mllib.linalg.DenseVector

/**
  * Created by dirceu on 26/05/16.
  */
class TestForecastPredictor extends FlatSpec with Matchers with BeforeAndAfterWithContext {
  this: Suite =>

  lazy val arimaData = List(
    Row(1d, 500d, 900),
    Row(1d, 505d, 880),
    Row(1d, 507d, 1000),
    Row(1d, 509d, 1500),
    Row(1d, 510d, 6516),
    Row(1d, 513d, 1650),
    Row(1d, 520d, 1050),
    Row(1d, 540d, 1835),
    Row(1d, 560d, 1358),
    Row(1d, 400d, 1688),
    Row(1d, 450d, 258),
    Row(1d, 590d, 384),
    Row(1d, 505d, 1982),
    Row(1d, 100d, 2880),
    Row(1d, 800d, 8000),
    Row(1d, 800d, 3518),
    Row(2d, 500d, 900),
    Row(2d, 512d, 8280),
    Row(2d, 507d, 10000),
    Row(2d, 509d, 10500),
    Row(2d, 610d, 616),
    Row(2d, 513d, 150),
    Row(2d, 520d, 1050),
    Row(2d, 540d, 135),
    Row(2d, 560d, 358),
    Row(2d, 400d, 688),
    Row(2d, 450d, 2258),
    Row(2d, 590d, 3184),
    Row(2d, 1500d, 982),
    Row(2d, 5100d, 880),
    Row(2d, 800d, 800),
    Row(2d, 800d, 3518)
  )
  lazy val data = List(
    Row(1d, 500d, 900, true),
    Row(1d, 505d, 880, true),
    Row(1d, 507d, 1000, true),
    Row(1d, 509d, 1500, true),
    Row(1d, 510d, 6516, true),
    Row(1d, 513d, 1650, true),
    Row(1d, 520d, 1050, true),
    Row(1d, 540d, 1835, true),
    Row(1d, 560d, 1358, true),
    Row(1d, 400d, 1688, true),
    Row(1d, 450d, 0, false),
    Row(1d, 590d, 384, true),
    Row(1d, 505d, 1982, true),
    Row(1d, 100d, 2880, true),
    Row(1d, 800d, 8000, true),
    Row(1d, 800d, 3518, true),
    Row(2d, 500d, 900, true),
    Row(2d, 512d, 8280, true),
    Row(2d, 507d, 10000, true),
    Row(2d, 509d, 10500, true),
    Row(2d, 610d, 616, true),
    Row(2d, 513d, 0, false),
    Row(2d, 520d, 1050, true),
    Row(2d, 540d, 135, true),
    Row(2d, 560d, 358, true),
    Row(2d, 400d, 688, true),
    Row(2d, 450d, 2258, true),
    Row(2d, 590d, 3184, true),
    Row(2d, 1500d, 982, true),
    Row(2d, 5100d, 880, true),
    Row(2d, 800d, 800, true),
    Row(2d, 800d, 3518, true)
  )

  lazy val testData = List(
    Row(1d, 1d, 1, true),
    Row(1d, 2d, 2, true),
    Row(1d, 3d, 3, true),
    Row(1d, 4d, 4, true),
    Row(1d, 5d, 5, true),
    Row(1d, 6d, 6, true),
    Row(1d, 7d, 7, true),
    Row(1d, 8d, 8, true),
    Row(1d, 9d, 9, false),
    Row(1d, 10d, 10, true),
    Row(1d, 11d, 11, true),
    Row(1d, 12d, 12, true),
    Row(1d, 13d, 13, true),
    Row(1d, 14d, 14, true),
    Row(1d, 15d, 15, true),
    Row(1d, 16d, 16, true),
    Row(2d, 17d, 17, true),
    Row(2d, 18d, 18, true),
    Row(2d, 19d, 19, false),
    Row(2d, 20d, 20, true),
    Row(2d, 21d, 21, true),
    Row(2d, 22d, 22, true),
    Row(2d, 23d, 23, true),
    Row(2d, 24d, 24, true),
    Row(2d, 25d, 25, true),
    Row(2d, 26d, 26, true),
    Row(2d, 27d, 27, true),
    Row(2d, 28d, 28, false),
    Row(2d, 29d, 29, true),
    Row(2d, 30d, 30, true),
    Row(2d, 31d, 31, true),
    Row(2d, 32d, 32, true)
  )

  val testDataWithoutOpen = testData.map(f => Row(f.toSeq.take(3): _*))
  val groupedList = data ++ List(
    Row(3d, 500d, 1900, true),
    Row(3d, 505d, 2880, true),
    Row(3d, 507d, 13000, true),
    Row(3d, 509d, 14500, true),
    Row(3d, 510d, 56516, true),
    Row(3d, 513d, 11650, true),
    Row(3d, 520d, 11050, true),
    Row(3d, 540d, 13835, true),
    Row(3d, 560d, 41358, true),
    Row(3d, 400d, 51688, true),
    Row(3d, 450d, 6258, true),
    Row(3d, 590d, 0, false),
    Row(3d, 505d, 71982, true),
    Row(3d, 100d, 52880, true),
    Row(3d, 800d, 28000, true),
    Row(3d, 800d, 32518, true),
    Row(4d, 500d, 9100, true),
    Row(4d, 514d, 68280, true),
    Row(4d, 507d, 110000, true),
    Row(4d, 509d, 510500, true),
    Row(4d, 610d, 6616, true),
    Row(4d, 513d, 0, false),
    Row(4d, 520d, 61050, true),
    Row(4d, 540d, 62135, true),
    Row(4d, 560d, 0, false),
    Row(4d, 400d, 688, true),
    Row(4d, 450d, 212258, true),
    Row(4d, 590d, 36184, true),
    Row(4d, 1500d, 4982, true),
    Row(4d, 5100d, 11880, true),
    Row(4d, 800d, 86600, true),
    Row(4d, 800d, 31518, true)
  )

  val groupedArimaList = groupedList.map(f => Row(f.toSeq.take(3): _*))

  val groupedTest = testData ++ List(
    Row(3d, 35d, 41, true),
    Row(3d, 36d, 42, true),
    Row(3d, 37d, 43, true),
    Row(3d, 38d, 44, true),
    Row(3d, 39d, 45, true),
    Row(3d, 40d, 46, true),
    Row(3d, 41d, 47, true),
    Row(3d, 58d, 48, true),
    Row(3d, 59d, 49, false),
    Row(3d, 60d, 50, true),
    Row(3d, 61d, 51, true),
    Row(3d, 62d, 52, true),
    Row(3d, 63d, 53, true),
    Row(3d, 64d, 54, true),
    Row(3d, 65d, 55, true),
    Row(3d, 66d, 56, false),
    Row(4d, 67d, 57, true),
    Row(4d, 68d, 58, true),
    Row(4d, 69d, 59, true),
    Row(4d, 70d, 60, true),
    Row(4d, 71d, 61, true),
    Row(4d, 72d, 62, true),
    Row(4d, 73d, 63, true),
    Row(4d, 74d, 64, true),
    Row(4d, 75d, 65, false),
    Row(4d, 76d, 66, true),
    Row(4d, 77d, 67, true),
    Row(4d, 78d, 68, true),
    Row(4d, 79d, 69, true),
    Row(4d, 80d, 70, true),
    Row(4d, 81d, 71, true),
    Row(4d, 82d, 72, true))

  val groupedArimaTest = groupedTest.map(f => Row(f.toSeq.take(3): _*))

  "ForecastPredictor" should "execute mean average and return predictions" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("label", DoubleType), StructField("date", DoubleType),
      StructField("features", IntegerType), StructField("Open", BooleanType)))

    val rdd = sc.parallelize(data)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().
      prepareMovingAveragePipeline[Double, Double](windowSize = 6)
    val model = timeSeriesBestModelFinder.fit(dataFrame)
    val df = model.transform(dataFrame)

    val first = df.first
    assert(first.getAs[org.apache.spark.mllib.linalg.Vector](1).toArray.length == 11)
    assert(first.getAs[Iterable[Double]](2).toArray.length == 11)
  }

  it should "execute ARIMA and return predictions" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("label", DoubleType), StructField("date", DoubleType),
      StructField("feat", IntegerType)))

    val rdd = sc.parallelize(arimaData)
    val dataFrame = sqlContext.createDataFrame(rdd, structType)

    val timeSeriesBestModelFinder = ForecastPredictor().prepareARIMAPipeline[Double, Double](featuresCol = "feat",
      nFutures = 5)
    val model = timeSeriesBestModelFinder.fit(dataFrame)
    val df = model.transform(dataFrame)
    val first = df.first
    assert(first.getAs[org.apache.spark.mllib.linalg.Vector]("validation").toArray.length == 5)
    assert(first.getAs[org.apache.spark.mllib.linalg.Vector]("featuresPrediction").toArray.length == 16)
  }

    it should "execute ARIMA without standard field names and return predictions" in {
      @transient val sc = context.sparkContext
      @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Sales", IntegerType), StructField("Open", BooleanType)))

      val rdd = sc.parallelize(arimaData)
      val dataFrame = sqlContext.createDataFrame(rdd, structType).filter("Sales !=0")

      val timeSeriesBestModelFinder = ForecastPredictor().prepareARIMAPipeline[Double, Double](labelCol = "Store",
        timeCol = "data", featuresCol = "Sales", nFutures = 5)
      val model = timeSeriesBestModelFinder.fit(dataFrame)
      val df = model.transform(dataFrame)
      val first = df.first
      assert(first.getAs[org.apache.spark.mllib.linalg.Vector]("validation").toArray.length == 5)
      assert(first.getAs[org.apache.spark.mllib.linalg.Vector]("featuresPrediction").toArray.length == 16)
    }


    it should "execute holtWinters and return predictions" in {
      @transient val sc = context.sparkContext
      @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("label", DoubleType), StructField("date", DoubleType),
      StructField("features", IntegerType), StructField("Open", BooleanType)))

      val rdd = sc.parallelize(data)
      val dataFrame = sqlContext.createDataFrame(rdd, structType)

      val timeSeriesBestModelFinder = ForecastPredictor().prepareHOLTWintersPipeline[Double, Double](nFutures = 8)
      val model = timeSeriesBestModelFinder.fit(dataFrame)
      val df = model.transform(dataFrame)
      val first = df.first
      assert(first.getAs[DenseVector]("validation").toArray.length == 8)
      assert(first.getAs[org.apache.spark.mllib.linalg.Vector]("features").toArray.length == 16)
    }

    it should "predict with ARIMA without standard field names and return predictions" in {
      @transient val sc = context.sparkContext
      @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Sales", IntegerType)))
      val testStructType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
        StructField("Id", IntegerType)))
      val rdd = sc.parallelize(arimaData)
      val testRdd = sc.parallelize(testDataWithoutOpen)
      val dataFrame = sqlContext.createDataFrame(rdd, structType)
      val testDataFrame = sqlContext.createDataFrame(testRdd, testStructType)

      val (timeSeriesBestModelFinder, model) = ForecastPredictor().predict[Double, Double, Int](dataFrame, testDataFrame,
        "Store", "Sales", "data", "Id", SupportedAlgorithm.Arima, 5)
      val first = timeSeriesBestModelFinder.collect
      val arima = model.stages.last.asInstanceOf[ArimaModel[Int]]
      val bestArima = arima.models.sortBy(_._2._2.minBy(_.metricResult).metricResult).first()
      val min = bestArima._2._2.minBy(_.metricResult)
      assert(first.length == 10)
      assert(model.stages.last.isInstanceOf[ArimaModel[Int]])
      assert(min.metricResult<1.7d)
    }


    it  should "choose the best model for each group" in {
      @transient val sc = context.sparkContext
      @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Sales", IntegerType)))
      val rdd = sc.parallelize(groupedArimaList)
      val dataFrame = sqlContext.createDataFrame(rdd, structType).filter("Sales !=0")

      val pipeline= ForecastPredictor().prepareBestForecastPipeline[Int, Int]("Store",
        "Sales", "validation","data", 5, Seq(8,12,16,24,26),(0 to 2).toArray)
      val model = pipeline.fit(dataFrame)
      val result = model.transform(dataFrame)
      assert(result.collect().length == 4)
      assert(result.select(IUberdataForecastUtil.ALGORITHM).distinct().count() >1)
    }

    it  should   "choose the best model for each group in predict method" in {
      @transient val sc = context.sparkContext
      @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Sales", IntegerType), StructField("Open", BooleanType)))
      val rdd = sc.parallelize(groupedList)
      val trainDf = sqlContext.createDataFrame(rdd, structType).filter("Sales !=0")
    val testStructType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Id", IntegerType), StructField("Open", BooleanType)))
      val testRdd = sc.parallelize(groupedTest)
      val testDf = sqlContext.createDataFrame(testRdd, testStructType)

      val (result,_) = ForecastPredictor().predict[Double,Int, Int](trainDf, testDf, "Store", "Sales", "data","Id",
        SupportedAlgorithm.FindBestForecast, 5, Seq(8,12,16,24,26))

      assert(result.collect().length == 20)
      assert(result.map(_.getAs[String](IUberdataForecastUtil.ALGORITHM)).distinct().count() >1)
    }

    "XGBoost" should  "execute a prediction with a simple dataset" in {
      @transient val sc = context.sparkContext
      @transient val sqlContext = context.sqlContext

    val structType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Sales", IntegerType),StructField("Open", BooleanType)))
      val rdd = sc.parallelize(groupedList)
      val trainDf = sqlContext.createDataFrame(rdd, structType)
      val trainDfDouble = trainDf.withColumn("Sales", trainDf("Sales").cast(DoubleType))

    val testStructType = StructType(Seq(StructField("Store", DoubleType), StructField("data", DoubleType),
      StructField("Id", IntegerType),StructField("Open", BooleanType)))
      val testRdd = sc.parallelize(groupedTest)
      val testDf = sqlContext.createDataFrame(testRdd, testStructType)

      val (result,_) = ForecastPredictor().predictSmallModelFeatureBased[Double,Int, Int](trainDfDouble, testDf,
        "Sales", "Store", "data","Id",SupportedAlgorithm.XGBoostAlgorithm, "validationCol")

      assert(result.collect().length == 64)
    }

    it should "execute prediction with rosenn dataset" in {
      val train = Dataset(context, s"$defaultFilePath/data/rosenntraintest.csv")
          .applyColumnTypes(Seq(IntegerType,IntegerType,TimestampType,IntegerType,IntegerType,IntegerType,IntegerType,
          StringType,StringType)).formatDateValues ("Date",DayMonthYear).
        applyColumnTypes(Seq(DoubleType,DoubleType,DoubleType,DoubleType,DoubleType,DoubleType,DoubleType,DoubleType,
          DoubleType,StringType,StringType)).select("Store","Sales","DayOfWeek","Date1","Date2","Date3","Open","Promo",
        "StateHoliday","SchoolHoliday").cache
      val test = Dataset(context, s"$defaultFilePath/data/rosenntesttest.csv")
        .applyColumnTypes(Seq(DoubleType,DoubleType,DoubleType,TimestampType,IntegerType,IntegerType,StringType,StringType))
        .formatDateValues ("Date",DayMonthYear).applyColumnTypes(Seq(DoubleType,DoubleType,DoubleType,DoubleType,
        DoubleType,DoubleType,DoubleType,DoubleType,StringType,StringType))

      val trainSchema = train.schema
      val testSchema = test.schema
      val sqlContext = context.sqlContext
      val convertedTest = sqlContext.createDataFrame(test.map{row =>
        val seq = row.toSeq
        val newSeq = if(seq.contains(null)){
          if(row.getAs[String]("StateHoliday") == "1.0")
            seq.updated(6,0d)
          else seq.updated(6,1d)
        }else seq
        Row(newSeq:_*)
      },testSchema)
      val convertedTrain = sqlContext.createDataFrame(train.map{row =>
        val seq = row.toSeq
        val newSeq = if(seq.contains(null)){
          if(row.getAs[String]("StateHoliday") == "1.0")
            seq.updated(6,0d)
          else seq.updated(6,1d)
        }else seq
        Row(newSeq:_*)
      },trainSchema)
      val (bestDf,_) = eleflow.uberdata.ForecastPredictor().
        predictSmallModelFeatureBased[Long,java.sql.Timestamp,Long](convertedTrain,convertedTest,"Sales","Store",
        "Date1","Id",XGBoostAlgorithm,"validacaocoluna")

      val cachedDf = bestDf.cache

      assert(cachedDf.count == 288)
      assert(train.count == 9420)
      assert(test.count == 288)
    }
}
