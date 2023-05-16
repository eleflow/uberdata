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

import eleflow.uberdata.core.data.UberDataset
import org.apache.spark.rpc.netty.BeforeAndAfterWithContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.util.Objects

/**
  * Created by dirceu on 22/10/14.
  */
class TestFilterColumns extends AnyFlatSpec with should.Matchers with BeforeAndAfterWithContext {

  import UberDataset._

  "FilterColumns" should
    "handle included string columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField("double", DoubleType, false)))
    val data = List(Row(1, 5, "vlr1", 10.5), Row(2, 1, "vl3", 0.1), Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.sliceByName(Seq("id", "string2"), Seq.empty)

//    assert(
//      result.collect.deep == Array(
//        new GenericRow(Array(1, "vlr1")),
//        new GenericRow(Array(2, "vl3")),
//        new GenericRow(Array(3, "vl3"))).deep)
    assert(Objects.deepEquals(
      result.collect, Array(
        new GenericRow(Array(1, "vlr1")),
        new GenericRow(Array(2, "vl3")),
        new GenericRow(Array(3, "vl3")))))
  }

  it should "correct handle excluded string columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField("double", DoubleType, false)))

    val data = List(Row(1, 5, "vlr1", 10.5), Row(2, 1, "vl3", 0.1), Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.sliceByName(excludes = Seq("id", "double"))
//    assert(
//      result.collect.deep == Array(
//        new GenericRow(Array(5, "vlr1")),
//        new GenericRow(Array(1, "vl3")),
//        new GenericRow(Array(8, "vl3"))).deep)
    assert(Objects.deepEquals(
      result.collect, Array(
        new GenericRow(Array(5, "vlr1")),
        new GenericRow(Array(1, "vl3")),
        new GenericRow(Array(8, "vl3")))))}

  it should "handle included int columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField("double", DoubleType, false)))
    val data = List(Row(1, 5, "vlr1", 10.5), Row(2, 1, "vl3", 0.1), Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.slice(Seq(2, 3))
//    assert(
//      result.collect.deep == Array(
//        new GenericRow(Array("vlr1", 10.5)),
//        new GenericRow(Array("vl3", 0.1)),
//        new GenericRow(Array("vl3", 10.0))).deep)
    assert(Objects.deepEquals(
      result.collect,Array(
        new GenericRow(Array("vlr1", 10.5)),
        new GenericRow(Array("vl3", 0.1)),
        new GenericRow(Array("vl3", 10.0)))))
  }

  it should "correct handle excluded int columns" in {
    @transient val uberContext = context
    @transient val sc = context.sparkContext
    @transient val sqlContext = context.sqlContext

    val structType = StructType(
      Seq(
        StructField("id", IntegerType, false),
        StructField("int", IntegerType, false),
        StructField("string2", StringType, false),
        StructField("double", DoubleType, false)))

    val data = List(Row(1, 5, "vlr1", 10.5), Row(2, 1, "vl3", 0.1), Row(3, 8, "vl3", 10.0))
    val rdd = sc.parallelize(data)
    val schema = sqlContext.applySchema(rdd, structType)
    val result = schema.slice(excludes = Seq(0, 3))
//    assert(
//      result.collect.deep == Array(
//        new GenericRow(Array(5, "vlr1")),
//        new GenericRow(Array(1, "vl3")),
//        new GenericRow(Array(8, "vl3"))).deep)
    assert(Objects.deepEquals(
      result.collect, Array(
        new GenericRow(Array(5, "vlr1")),
        new GenericRow(Array(1, "vl3")),
        new GenericRow(Array(8, "vl3")))))
  }
}
