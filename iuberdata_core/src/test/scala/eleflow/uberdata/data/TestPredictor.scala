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

import eleflow.uberdata.core.util.ClusterSettings
import eleflow.uberdata.core.data.Dataset
import eleflow.uberdata.core.exception.InvalidDataException
import eleflow.uberdata.data.stat.Statistics
import eleflow.uberdata.enums.SupportedAlgorithm._
import eleflow.uberdata.enums.ValidationMethod
import eleflow.uberdata.model.TypeMixin.TrainedData
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint, RegressionModel}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.easymock.EasyMock
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by dirceu on 03/11/14.
  */
class TestPredictor extends FlatSpec with Matchers with BeforeAndAfterWithContext {

  "DataPredictor" should "throw an exception if included and excluded are empty" in {

    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val data = List(
      Row(1, 5, "vlr1", BigDecimal(10.5)),
      Row(2, 1, "vl3", BigDecimal(0.1)),
      Row(3, 8, "vl3", BigDecimal(10.0)),
      Row(4, 1, "vl4", BigDecimal(1.0)))
    val rdd = sc.parallelize(data)
    assert(rdd.count == 4)
    val schema = sqlContext.applySchema(rdd, structType)

    intercept[IllegalArgumentException] {
      Predictor.predict(schema, schema, algorithm = ToBeDetermined, iterations = 10)
    }
  }
  it should "select the appropriated binary algorithm" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))
    val testStructType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val data = List(
      Row(10, 0, "vlr1", BigDecimal(10.5)),
      Row(20, 1, "vl3", BigDecimal(0.1)),
      Row(30, 0, "vl3", BigDecimal(10.0)),
      Row(40, 1, "vl4", BigDecimal(1.0)),
      Row(50, 0, "vl3", BigDecimal(10.0)),
      Row(60, 1, "vl4", BigDecimal(1.0)),
      Row(70, 1, "vlr1", BigDecimal(12.0)),
      Row(80, 1, "vlr1", BigDecimal(10.0)),
      Row(90, 0, "vl4", BigDecimal(15.0)),
      Row(100, 0, "vl3", BigDecimal(10.0)))
    val rdd = sc.parallelize(data)
    val testRdd = rdd.map(f => Row(f(0), f(2), f(3)))
    val schema = sqlContext.applySchema(rdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testStructType)

    val model = EasyMock.createMock(classOf[TrainedData[SVMModel]])

    object TestPredictor extends Predictor {
      override def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                     algorithm: Algorithm,
                                     iterations: Int = 100) = {
        assert(algorithm == BinarySupportVectorMachines)
        model
      }
    }

    EasyMock.replay(model)

    val prediction = Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(1),
      includes = Seq(0, 1, 2, 3),
      algorithm = ToBeDetermined,
      iterations = 10)
    prediction.testPredictionId.collect

    EasyMock.verify(model)
  }

  it should "select the appropriated binary algorithm logistic regression" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))
    val testStructType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val data = List(
      Row(1, 0, "vlr1", BigDecimal(10.5)),
      Row(2, 1, "vl3", BigDecimal(0.1)),
      Row(3, 0, "vl3", BigDecimal(10.0)),
      Row(4, 1, "vl4", BigDecimal(1.0)),
      Row(5, 0, "vlr1", BigDecimal(10.5)),
      Row(6, 1, "vl3", BigDecimal(0.1)),
      Row(7, 0, "vl3", BigDecimal(10.0)),
      Row(8, 1, "vl4", BigDecimal(1.0)),
      Row(9, 1, "vl3", BigDecimal(10.0)),
      Row(10, 0, "vlr1", BigDecimal(10.5)))
    val rdd = sc.parallelize(data)
    val testRdd = rdd.map(f => Row(f(0), f(2), f(3)))
    val schema = sqlContext.applySchema(rdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testStructType)

    val model = EasyMock.createMock(classOf[TrainedData[SVMModel]])

    object TestPredictor extends Predictor {
      override def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                     algorithm: Algorithm,
                                     iterations: Int = 100) = {
        assert(algorithm == BinaryLogisticRegressionBFGS)
        model
      }
    }

    EasyMock.replay(model)

    Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(1),
      includes = Seq(0, 1, 2, 3),
      algorithm = BinaryLogisticRegressionBFGS,
      iterations = 10)

    EasyMock.verify(model)
  }

  it should "test correctly load avazu test data" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    import sqlContext._

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      Avazu(1000009418151094270d, 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864000d, 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120000d, 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838000d, 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042000d, 0, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTest(1000009418151094270d, 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(10000169349117864000d, 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(10000371904215120000d, 14132100, 1005, 2, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 2, 15706, 320, 50, 2161, 0, 35),
      AvazuTest(10000640724480838000d, 14104100, 1005, 4, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      AvazuTest(10000679056417042000d, 14101100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 2, 18993, 320, 50, 2161, 0, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(
      f =>
        Row(
          f.id,
          f.click,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val testRdd = testrdd.map(
      f =>
        Row(
          f.id,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val model = EasyMock.createMock(classOf[TrainedData[SVMModel]])

    object TestPredictor extends Predictor {
      override def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                     algorithm: Algorithm,
                                     iterations: Int = 100) = {
        assert(algorithm == BinarySupportVectorMachines)
        model
      }
    }

    EasyMock.replay(model)

    Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(1),
      excludes = Seq(11, 12),
      algorithm = ToBeDetermined,
      iterations = 10)

    EasyMock.verify(model)
  }
  it should "only one value can't be predicted" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val trainStruct = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))
    val testStruct = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))
    val train = List(
      Row(1, 5, "vlr1", BigDecimal(10.5)),
      Row(2, 5, "vl3", BigDecimal(0.1)),
      Row(3, 5, "vl3", BigDecimal(10.0)))
    val test = List(
      Row(1, "vlr1", BigDecimal(10.5)),
      Row(2, "vl3", BigDecimal(0.1)),
      Row(3, "vl3", BigDecimal(10.0)))
    val trainRdd = sc.parallelize(train)
    val trainDf = sqlContext.applySchema(trainRdd, trainStruct)
    val testRdd = sc.parallelize(test)
    val testDf = sqlContext.applySchema(testRdd, testStruct)

    val model = EasyMock.createMock(classOf[GeneralizedLinearModel])
    val ret: Double = 0.5
    EasyMock
      .expect(model.predict(EasyMock.anyObject[org.apache.spark.ml.linalg.Vector]()))
      .andReturn(ret)
      .anyTimes()

    EasyMock.replay(model)

    object TestPredictor extends Predictor {
      override def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                     algorithm: Algorithm,
                                     iterations: Int = 100) = {
        assert(algorithm == BinarySupportVectorMachines)
        TrainedData[SVMModel](model, algorithm, iterations)
      }
    }

    intercept[InvalidDataException] {
      TestPredictor.predictInt(
        trainDf,
        testDf,
        includes = Seq(0, 1, 2),
        algorithm = ToBeDetermined,
        iterations = 10)
    }

    EasyMock.verify(model)
  }

//  it should "adapt the included/excluded columns greater than the target index of the schema for testdataset" in {
//    val target = Seq("id", "string")
//    val included = Seq("click", "val1", "val2", "val3", "val4", "val5")
//    val result = Predictor.updateTestFields(included, target)
//    assert(result == Seq(1, "id", "val2", "val3", "val4", "val5"))

//  }

  it should "execute predictMultiple models" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    import sqlContext._

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      Avazu(1000009418151094270d, 0L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864000d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120000d, 0L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838000d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042000d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(10000679056417042001d, 1L, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35),
      Avazu(1000009418151094272d, 0L, 14102100, 1005, 0, "fe8cc448", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864003d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120004d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838005d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042006d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(10000679056417042007d, 1L, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTest(1000009418151094270d, 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(10000169349117864000d, 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(10000371904215120000d, 14132100, 1005, 2, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 2, 15706, 320, 50, 2161, 0, 35),
      AvazuTest(10000640724480838000d, 14104100, 1005, 4, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      AvazuTest(10000679056417042000d, 14101100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 2, 18993, 320, 50, 2161, 0, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(
      f =>
        Row(
          f.id,
          f.click,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val testRdd = testrdd.map(
      f =>
        Row(
          f.id,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val multiplePrediction = Predictor.predictMultiple(
      schema,
      testSchema,
      target = Seq(1),
      excludes = Seq(11, 12),
      algorithm = List(
        BinaryLogisticRegressionSGD,
        LinearLeastSquares,
        NaiveBayesClassifier,
        BinarySupportVectorMachines),
      iterations = 1)

    val models = multiplePrediction.models;

    assert(models(0).model.isInstanceOf[LogisticRegressionModel])
    assert(models(1).model.isInstanceOf[RegressionModel])
    assert(models(2).model.isInstanceOf[NaiveBayesModel])
    assert(models(3).model.isInstanceOf[SVMModel])
    //assert(testSchema.count == prediction.count)
  }

  it should "execute predictMultiple models and compare with single predict" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    import sqlContext._

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      Avazu(1000009418151094270d, 0L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864000d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120000d, 0L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838000d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042000d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(10000679056417042001d, 1L, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35),
      Avazu(1000009418151094272d, 0L, 14102100, 1005, 0, "fe8cc448", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864003d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120004d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838005d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042006d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(10000679056417042007d, 1L, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTest(1000009418151094270d, 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(10000169349117864000d, 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(10000371904215120000d, 14132100, 1005, 2, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 2, 15706, 320, 50, 2161, 0, 35),
      AvazuTest(10000640724480838000d, 14104100, 1005, 4, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      AvazuTest(10000679056417042000d, 14101100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 2, 18993, 320, 50, 2161, 0, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(
      f =>
        Row(
          f.id,
          f.click,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val testRdd = testrdd.map(
      f =>
        Row(
          f.id,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    object TestPredictor extends Predictor {
      override def splitRddInTestValidation[T](dataSet: RDD[T],
                                               train: Double = 0.7): (RDD[T], RDD[T]) = {
        require(train < 1d)
        require(train > 0)
        val split = dataSet.randomSplit(Array(train, 1 - train), 1L)
        split match {
          case Array(a: RDD[T], b: RDD[T]) => (a, b)
          case _ => throw new InvalidDataException(s"Unexpected split data result ")
        }
      }
    }

    val multiplePrediction = TestPredictor.predictMultiple(
      schema,
      testSchema,
      target = Seq(1),
      excludes = Seq(11, 12),
      algorithm = List(BinaryLogisticRegressionSGD, BinarySupportVectorMachines),
      iterations = 10)

    val singlePrediction = TestPredictor.predict(
      schema,
      testSchema,
      target = Seq("click"),
      excludes = Seq("device_ip", "device_model"),
      algorithm = BinarySupportVectorMachines,
      iterations = 10)

    val validationMultiple =
      multiplePrediction.multiplePredictionValidation.get(BinarySupportVectorMachines).get
    val validationSingle = singlePrediction.validationPrediction

    val multipleLogLoss = Statistics.logarithmicLoss(validationMultiple)
    val singleLogLoss = Statistics.logarithmicLoss(validationSingle)

    assert((multipleLogLoss / singleLogLoss) > 0.9 && (multipleLogLoss / singleLogLoss) < 1.1)
    val models = multiplePrediction.models;

    assert(models(0).model.isInstanceOf[LogisticRegressionModel])
    assert(models(1).model.isInstanceOf[SVMModel])
  }

  it should "execute predictsvm" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    import sqlContext._

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      Avazu(1000009418151094270d, 0L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864000d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120000d, 0L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838000d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042000d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(10000679056417042001d, 1L, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35),
      Avazu(1000009418151094272d, 0L, 14102100, 1005, 0, "fe8cc448", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000169349117864003d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000371904215120004d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(10000640724480838005d, 1L, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(10000679056417042006d, 0L, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(10000679056417042007d, 1L, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTest(BigDecimal(1000009418151094270d), 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(BigDecimal(10000169349117864000d), 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(BigDecimal(10000371904215120000d), 14132100, 1005, 2, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 2, 15706, 320, 50, 2161, 0, 35),
      AvazuTest(BigDecimal(10000640724480838000d), 14104100, 1005, 4, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      AvazuTest(BigDecimal(10000679056417042000d), 14101100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 2, 18993, 320, 50, 2161, 0, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(
      f =>
        Row(
          f.id,
          f.click,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val testRdd = testrdd.map(
      f =>
        Row(
          f.id,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val prediction = Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(1),
      excludes = Seq(11, 12),
      algorithm = BinarySupportVectorMachines,
      iterations = 10)
    val model = prediction.model

    assert(model.isInstanceOf[TrainedData[_]])
    assert(model.asInstanceOf[TrainedData[_]].model.isInstanceOf[SVMModel])
  }

  it should "execute decisionTree" in {
    @transient val uberContext = context
    @transient val sc = uberContext.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("C1", IntegerType, false),
        StructField("banner_pos", IntegerType, false),
        StructField("site_id", StringType, false),
        StructField("site_domain", StringType, false),
        StructField("site_category", StringType, false),
        StructField("app_id", StringType, false),
        StructField("app_domain", StringType, false),
        StructField("app_category", StringType, false),
        StructField("device_id", StringType, false),
        StructField("device_ip", StringType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("device_conn_type", IntegerType, false),
        StructField("C14", IntegerType, false),
        StructField("C15", IntegerType, false),
        StructField("C16", IntegerType, false),
        StructField("C17", IntegerType, false),
        StructField("C18", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      Avazu(BigDecimal(1000009418151094270d), 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 15706, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000169349117864000d), 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000371904215120000d), 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000640724480838000d), 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000679056417042000d), 0, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(BigDecimal(10000371904215120001d), 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000640724480838001d), 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000679056417042001d), 0, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(BigDecimal(10000169349117864002d), 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000371904215120002d), 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000640724480838002d), 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000679056417042002d), 0, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(BigDecimal(10000371904215120003d), 0, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 0, 15704, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000640724480838003d), 1, 14102100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      Avazu(BigDecimal(10000679056417042003d), 0, 14102100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 18993, 320, 50, 2161, 0, 35),
      Avazu(BigDecimal(10000679056417042001d), 1, 14102100, 1005, 0, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 0, 15706, 320, 50, 2161, 0, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTest(BigDecimal(1000009418151094270d), 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "ddd2926e", "44956a24", 1, 2, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(BigDecimal(10000169349117864000d), 14202100, 1005, 0, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "96809ac8", "711ee120", 1, 0, 18993, 320, 50, 1722, 0, 35),
      AvazuTest(BigDecimal(10000371904215120000d), 14132100, 1005, 2, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "b3cf8def", "8a4875bd", 1, 2, 15706, 320, 50, 2161, 0, 35),
      AvazuTest(BigDecimal(10000640724480838000d), 14104100, 1005, 4, "1fbe01fe", "f3845767", "28905ebd", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "e8275b8f", "6332421a", 1, 0, 15706, 320, 50, 1722, 0, 35),
      AvazuTest(BigDecimal(10000679056417042000d), 14101100, 1005, 1, "fe8cc448", "9166c161", "0569f928", "ecad2386", "7801e8d", "907d7df22", "a99f214a", "9644d0bf", "779d90c2", 1, 2, 18993, 320, 50, 2161, 0, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(
      f =>
        Row(
          f.id,
          f.click,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val testRdd = testrdd.map(
      f =>
        Row(
          f.id,
          f.hour,
          f.C1,
          f.banner_pos,
          f.site_id,
          f.site_domain,
          f.site_category,
          f.app_id,
          f.app_domain,
          f.app_category,
          f.device_id,
          f.device_ip,
          f.device_model,
          f.device_type,
          f.device_conn_type,
          f.C14,
          f.C15,
          f.C16,
          f.C17,
          f.C18,
          f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val prediction = Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(1),
      excludes = Seq(11, 12),
      algorithm = DecisionTreeAlg,
      iterations = 10)
    val model = prediction.model

    assert(model.model.isInstanceOf[DecisionTreeModel])
  }

  it should "throw exception if target or id is empty" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val data = List(
      Row(1, 5, "vlr1", BigDecimal(10.5)),
      Row(2, 1, "vl3", BigDecimal(0.1)),
      Row(3, 8, "vl3", BigDecimal(10.0)),
      Row(4, 1, "vl4", BigDecimal(1.0)))
    val rdd = sc.parallelize(data)
    assert(rdd.count == 4)
    val schema = sqlContext.applySchema(rdd, structType)

    intercept[IllegalArgumentException] {
      Predictor.predictInt(
        schema,
        schema,
        algorithm = ToBeDetermined,
        iterations = 10,
        includes = Seq(1, 2, 3, 4),
        target = Seq.empty)
    }
    intercept[IllegalArgumentException] {
      Predictor.predictInt(
        schema,
        schema,
        algorithm = ToBeDetermined,
        iterations = 10,
        includes = Seq(1, 2, 3, 4),
        ids = Seq.empty)
    }
  }

  it should "decision tree without categorization columns" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))
    val testStructType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val data = List(
      Row(1, 0, "vlr1", BigDecimal(10.5)),
      Row(2, 1, "vl3", BigDecimal(0.1)),
      Row(3, 0, "vl3", BigDecimal(10.0)),
      Row(4, 1, "vl4", BigDecimal(1.0)),
      Row(5, 0, "vlr1", BigDecimal(10.5)),
      Row(6, 1, "vl3", BigDecimal(0.1)),
      Row(7, 0, "vl3", BigDecimal(10.0)),
      Row(8, 1, "vl4", BigDecimal(1.0)),
      Row(9, 0, "vlr1", BigDecimal(10.5)),
      Row(10, 1, "vl3", BigDecimal(0.1)),
      Row(11, 0, "vl3", BigDecimal(10.0)),
      Row(12, 1, "vl4", BigDecimal(1.0)))
    val rdd = sc.parallelize(data)
    val testRdd = rdd.map(f => Row(f(0), f(2), f(3)))
    val schema = sqlContext.applySchema(rdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testStructType)

    val model = EasyMock.createMock(classOf[TrainedData[SVMModel]])

    object TestPredictor extends Predictor {
      override def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                     algorithm: Algorithm,
                                     iterations: Int = 100) = {
        assert(algorithm == BinaryLogisticRegressionBFGS)
        model
      }
    }

    EasyMock.replay(model)

    val prediction = Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(1),
      includes = Seq(0, 1, 3),
      algorithm = DecisionTreeAlg,
      iterations = 10)

    assert(prediction.model.model.isInstanceOf[DecisionTreeModel])

    EasyMock.verify(model)
  }

  it should "correct execute evolutive prediction with column steps" in {
    import Dataset._

    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      AvazuResumed(BigDecimal(1000009418151094270d), 0L, 14102100, 3, "44956a24", 0, 35),
      AvazuResumed(BigDecimal(10000169349117864000d), 1L, 14102100, 3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120000d), 0L, 14102100, 3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838000d), 1L, 14102100, 3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042000d), 0L, 14102100, 3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000371904215120001d), 0L, 14102100, 3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838001d), 1L, 14102100, 3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042001d), 0L, 14102100, 3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000169349117864002d), 1L, 14102100, 3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120002d), 0L, 14102100, 3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838002d), 1L, 14102100, 3, "711ee120", 5, 35),
      AvazuResumed(BigDecimal(10000679056417042002d), 0L, 14102100, 3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000371904215120003d), 0L, 14102100, 3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838003d), 1L, 14102100, 3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042003d), 0L, 14102100, 3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000679056417042001d), 1L, 14102100, 3, "711ee120", 1, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTestResumed(BigDecimal(1000009418151094270d), 14202100, 3, "44956a24", 0, 35),
      AvazuTestResumed(BigDecimal(10000169349117864000d), 14202100, 3, "711ee120", 1, 35),
      AvazuTestResumed(BigDecimal(10000371904215120000d), 14132100, 3, "8a4875bd", 0, 35),
      AvazuTestResumed(BigDecimal(10000640724480838000d), 14104100, 3, "6332421a", 0, 35),
      AvazuTestResumed(BigDecimal(10000679056417042000d), 14101100, 3, "711ee120", 1, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(f => Row(f.id, f.click, f.hour, f.device_model, f.device_type, f.C19))
    val testRdd = testrdd.map(f => Row(f.id, f.hour, f.device_model, f.device_type, f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val result = Predictor.evolutivePredictColumnList(
      schema,
      testSchema,
      algorithm = BinaryLogisticRegressionBFGS,
      validationMethod = ValidationMethod.LogarithmicLoss,
      iterations = 1,
      columnNStep = List(4, 3),
      ids = Seq("id"),
      target = Seq("click"))
    val columns = result.map(_._3)
    val values = columns.groupBy(_.size)
    val firstStep = values.get(1).get
    val secondStep = values.get(2).get
    val thirdStep = values.get(3).get
    assert(firstStep.size == 4)
    assert(secondStep.size == 6)
    assert(secondStep.reduceLeft(_ ++ _).toSet.size == 4)
    assert(thirdStep.size == 1)
    assert(thirdStep.head.toSet.size == 3)
  }

  it should "steps can't be empty" in {
    import Dataset._

    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext
    val data = List(
      Row(1, 5, "vlr1", BigDecimal(10.5)),
      Row(2, 1, "vl3", BigDecimal(0.1)),
      Row(3, 8, "vl3", BigDecimal(10.0)),
      Row(4, 1, "vl4", BigDecimal(1.0)))
    val rdd = sc.parallelize(data)

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val schema = sqlContext.applySchema(rdd, structType)

    intercept[IllegalArgumentException] {
      Predictor.evolutivePredict(
        schema,
        schema,
        algorithm = BinaryLogisticRegressionBFGS,
        validationMethod = ValidationMethod.LogarithmicLoss,
        iterations = 10,
        steps = List.empty)
    }
  }

  it should "predict with columnSetup" in {
    import Dataset._

    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      AvazuResumed(BigDecimal(1000009418151094270d), 0L, 14102100,3, "44956a24", 0, 35),
      AvazuResumed(BigDecimal(10000169349117864000d), 1L, 54102100,3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120000d), 0L, 14102100,3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838000d), 1L, 54102100,3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042000d), 0L, 14102100,3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000371904215120001d), 0L, 14102100,3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838001d), 1L, 54102100,3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042001d), 0L, 14102100,3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000169349117864002d), 1L, 54102100,3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120002d), 0L, 14102100,3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838002d), 1L, 54102100,3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042002d), 0L, 14102100,3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000371904215120003d), 0L, 14102100,3, "8a4875bd", 0, 35),
      AvazuResumed(BigDecimal(10000640724480838003d), 1L, 54102100,3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042003d), 0L, 14102100,3, "779d90c2", 0, 35),
      AvazuResumed(BigDecimal(10000679056417042001d), 1L, 54102100,3, "779d90c2", 1, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTestResumed(1000009418151094270d, 14202100,3, "44956a24", 1, 35),
      AvazuTestResumed(10000169349117864000d, 14202100,3, "711ee120", 1, 35),
      AvazuTestResumed(10000371904215120000d, 14132100,3, "8a4875bd", 1, 35),
      AvazuTestResumed(10000640724480838000d, 14104100,3, "6332421a", 1, 35),
      AvazuTestResumed(10000679056417042000d, 14101100,3, "779d90c2", 1, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(f => Row(f.id, f.click, f.hour, f.device_model, f.device_type, f.C19))
    val testRdd = testrdd.map(f => Row(f.id, f.hour, f.device_model, f.device_type, f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val (_, _, _, _, _, _, correlatedColumns) = Predictor.predictWithColumnSetup(
      schema,
      testSchema,
      algorithm = BinaryLogisticRegressionBFGS,
      validationMethod = ValidationMethod.LogarithmicLoss,
      selectColumns = Left(2),
      iterations = 1,
      target = Seq("click"))

    assert(correlatedColumns.size == 2)
    assert(correlatedColumns.sorted == Seq(0, 6))
  }

  it should "excute neuralnetwork predict" in {
    import Dataset._

    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("click", LongType, false),
        StructField("hour", IntegerType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("C19", IntegerType, false)))
    val testType = StructType(
      Seq(
        StructField(
          "id",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false),
        StructField("hour", IntegerType, false),
        StructField("device_model", StringType, false),
        StructField("device_type", IntegerType, false),
        StructField("C19", IntegerType, false)))

    // format: off
    val data = List(
      AvazuResumed(BigDecimal(1000009418151094270d), 0L, 14102100, 3, "44956a24", 1, 35),
      AvazuResumed(BigDecimal(10000169349117864000d), 1L, 14102100,3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120000d), 0L, 14102100, 3,"8a4875bd", 1, 35),
      AvazuResumed(BigDecimal(10000640724480838000d), 1L, 14102100, 3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042000d), 0L, 14102100, 3, "779d90c2", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120001d), 0L, 14102100, 3, "8a4875bd", 1, 35),
      AvazuResumed(BigDecimal(10000640724480838001d), 1L, 14102100, 3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042001d), 0L, 14102100, 3, "779d90c2", 1, 35),
      AvazuResumed(BigDecimal(10000169349117864002d), 1L, 14102100, 3, "711ee120", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120002d), 0L, 14102100, 3, "8a4875bd", 1, 35),
      AvazuResumed(BigDecimal(10000640724480838002d), 1L, 14102100, 3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042002d), 0L, 14102100, 3, "779d90c2", 1, 35),
      AvazuResumed(BigDecimal(10000371904215120003d), 0L, 14102100, 3, "8a4875bd", 1, 35),
      AvazuResumed(BigDecimal(10000640724480838003d), 1L, 14102100, 3, "6332421a", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042003d), 0L, 14102100,3, "779d90c2", 1, 35),
      AvazuResumed(BigDecimal(10000679056417042001d), 1L, 14102100,3, "779d90c2", 1, 35)
    )
    val rdd = sc.parallelize(data)
    val testdata = List(
      AvazuTestResumed(BigDecimal(1000009418151094270d), 14202100,3, "44956a24", 1, 35),
      AvazuTestResumed(BigDecimal(10000169349117864000d), 14202100,3, "711ee120", 1, 35),
      AvazuTestResumed(BigDecimal(10000371904215120000d), 14132100,3, "8a4875bd", 1, 35),
      AvazuTestResumed(BigDecimal(10000640724480838000d), 14104100,3, "6332421a", 1, 35),
      AvazuTestResumed(BigDecimal(10000679056417042000d), 14101100,3, "779d90c2", 1, 35)
    )
    // format: on

    val testrdd = sc.parallelize(testdata)
    val trainRdd = rdd.map(f => Row(f.id, f.click, f.hour, f.device_model, f.device_type, f.C19))
    val testRdd = testrdd.map(f => Row(f.id, f.hour, f.device_model, f.device_type, f.C19))
    val schema = sqlContext.applySchema(trainRdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testType)

    val prediction = Predictor.predictInt(
      schema,
      testSchema,
      excludes = Seq(5),
      algorithm = ArtificialNeuralNetwork,
      iterations = 1)
    val model = prediction.model

    assert(model.isInstanceOf[TrainedData[_]])
    assert(model.asInstanceOf[TrainedData[_]].model.isInstanceOf[ANNClassifierModel])
  }

  it should "naivebayes with string columns" in {
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))
    val testStructType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField(
          "double",
          DecimalType(
            ClusterSettings.defaultDecimalPrecision,
            ClusterSettings.defaultDecimalScale),
          false)))

    val data = List(
      Row(1, 0, "vlr1", BigDecimal(10.5)),
      Row(2, 1, "vl3", BigDecimal(0.1)),
      Row(3, 0, "vl3", BigDecimal(10.0)),
      Row(4, 1, "vl4", BigDecimal(1.0)),
      Row(5, 0, "vlr1", BigDecimal(10.5)),
      Row(6, 1, "vl3", BigDecimal(0.1)),
      Row(7, 0, "vl3", BigDecimal(10.0)),
      Row(8, 1, "vl4", BigDecimal(1.0)),
      Row(9, 0, "vlr1", BigDecimal(10.5)),
      Row(10, 1, "vl3", BigDecimal(0.1)),
      Row(11, 0, "vl3", BigDecimal(10.0)),
      Row(12, 1, "vl4", BigDecimal(1.0)))
    val rdd = sc.parallelize(data)
    val testRdd = rdd.map(f => Row(f(0), f(1), f(3)))
    val schema = sqlContext.applySchema(rdd, structType)
    val testSchema = sqlContext.applySchema(testRdd, testStructType)

    val model = EasyMock.createMock(classOf[TrainedData[SVMModel]])

    object TestPredictor extends Predictor {
      override def trainForAlgorithm(trainDataSet: RDD[LabeledPoint],
                                     algorithm: Algorithm,
                                     iterations: Int = 100) = {
        assert(algorithm == BinaryLogisticRegressionBFGS)
        model
      }
    }

    val prediction = Predictor.predictInt(
      schema,
      testSchema,
      target = Seq(2),
      includes = Seq(0, 1, 3),
      algorithm = ToBeDetermined,
      iterations = 10)

    EasyMock.replay(model)

    assert(prediction.model.model.isInstanceOf[NaiveBayesModel])

    EasyMock.verify(model)
  }
}
