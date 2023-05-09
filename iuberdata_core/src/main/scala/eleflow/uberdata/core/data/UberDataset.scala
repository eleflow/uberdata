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

import eleflow.uberdata.core.IUberdataContext
import eleflow.uberdata.core.data.dataset._
import eleflow.uberdata.core.enums.DataSetType
import eleflow.uberdata.core.enums.DateSplitType._
import eleflow.uberdata.core.exception.{InvalidDataException, UnexpectedFileFormatException}
import eleflow.uberdata.core.io.IUberdataIO
import eleflow.uberdata.core.util.{ClusterSettings, DateTimeParser}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType, DataType => SqlDataType}
import org.joda.time.{DateTime, DateTimeZone, Days}
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.net.URI
import java.sql.Timestamp
import scala.collection.immutable.TreeSet

/**
 * SparkNotebook
 * Copyright (C) 2014 eleflow.
 * User: paulomagalhaes
 * Date: 11/4/14 3:44 PM
 */
object UberDataset {
	implicit def DatasetToDataFrame(dataset: UberDataset): DataFrame =
		dataset.toDataFrame

	implicit def DataFrameToDataset(dataFrame: DataFrame): UberDataset =
		new UberDataset(dataFrame)

	implicit def FileDatasetToDataset(fileDS: FileDataset): UberDataset =
		new UberDataset(fileDS.toDataFrame)

	implicit def FileDatasetToDataFrame(fileDS: FileDataset): DataFrame =
		fileDS.toDataFrame

	def apply(uc: IUberdataContext, file: String, dateTimeParser: DateTimeParser) = {
		new UberDataset(
			new FileDataset(uc, file, separator = ",").toDataFrame,
			dateTimeParser = dateTimeParser
		)
	}

	def apply(uc: IUberdataContext, file: String, separator: String = ",") = {
		new FileDataset(uc, file, separator)
	}
}

class UberDataset private[data](dataFrame: DataFrame,
																originalDataSet: Option[UberDataset] = None,
																defaultSummarizedColumns: Option[
															RDD[(Int, (Int, (Any) => Int, (Any) => Double))]
															] = None,
																label: Seq[String] = Seq.empty,
																dateTimeParser: DateTimeParser = DateTimeParser())
	extends Serializable {

	originalDataSet
		.map(f => dataFrameName(f.toDataFrame))
		.getOrElse(dataFrameName(dataFrame))
		.foreach(dataFrame.createOrReplaceTempView)

	lazy val columnsSize = summarizedColumns.map(_._2._1).sum().toInt
	lazy val summarizedColumns = defaultSummarizedColumns.getOrElse(
		summarizeColumns.setName("summarizedColumns").cache()
	)
	lazy val columnIndexOf = this.schema.fieldNames.zipWithIndex.toSet.toMap
	lazy val summarizedColumnsIndex = summarizeColumnsIndex
	lazy val labels =
		if (label.isEmpty) Seq(this.schema.fieldNames.head) else label

	def columnTypes(): Seq[SqlDataType] = {
		dataFrame.schema.fields.map(_.dataType)
	}

	def columnNames(): Seq[String] = {
		dataFrame.schema.fields.map(_.name)
	}

	def sliceByName(includes: Seq[String] = dataFrame.schema.fields.map(_.name),
									excludes: Seq[String] = Seq[String]()): UberDataset = {
		val includesIndices = dataFrame.schema.fields.zipWithIndex.collect {
			case (structField, index)
				if includes.contains(structField.name) && !excludes.contains(
					structField.name
				) =>
				index
		}
		slice(includesIndices, Seq[Int]())
	}

	def slice(includes: Seq[Int] = 0 to dataFrame.columns.length,
						excludes: Seq[Int] = Seq.empty[Int]): UberDataset = {
		val fields = dataFrame.columns.zipWithIndex.filter {
			case (columnName, index) =>
				includes.contains(index) && !excludes.contains(index)
		}.map(_._1)
		import org.apache.spark.sql.catalyst.dsl.expressions.symbolToUnresolvedAttribute
		val filtered = fields.map(x => symbolToUnresolvedAttribute(Symbol(x)).name)

		val newDataFrame =
			if (filtered.length > 1)
				dataFrame.select(filtered.head, filtered.tail: _*)
			else dataFrame.select(filtered.head)
		new UberDataset(newDataFrame, None)
	}

	lazy val convertedDataFrame = toDataFrame

	def toDataFrame: DataFrame = dataFrame

	//    convert(dataFrame, getDataFrameSchema)

	private def getDataFrameSchema = //if (!converted) structType() else
		dataFrame.schema

	def toLabeledPoint: RDD[LabeledPoint] = {
		DataTransformer
			.createLabeledPointFromRDD(
				dataFrame,
				Seq(),
				Seq(),
				summarizedColumns,
				DataSetType.Test,
				columnsSize
			)
			.values
	}

	def translateCorrelation(array: Array[(Double, Int)]) = {
		array.map { f =>
			summarizedColumns.map { g =>
				g
			}
		}
	}

	private def dataFrameName(dataFrame: DataFrame) = {
		dataFrame.schema.typeName match {
			case null => None
			//TODO test
			case _ => Some(dataFrame.schema.typeName)
		}
	}

	private def summarizeColumns = {


		val fieldsTuple = convertedDataFrame.dtypes.zipWithIndex.partition(
			f => f._1._2 == org.apache.spark.sql.types.StringType.toString
		)
		val (stringFields, nonStringFields) =
			(fieldsTuple._1.map(_._2), fieldsTuple._2.map(_._2))
		val valuex = convertedDataFrame.rdd.flatMap { row =>
			stringFields.map { sf =>
				(sf, TreeSet(row.getString(sf)))
			}
		}.reduceByKey(_ ++ _)
		val stringFieldsRdd: RDD[(Int, (Int, (Any => Int), (Any => Double)))] =
			valuex.map {
				case (index, values) =>
					index -> (values.size, values.zipWithIndex.map { f =>
						(f._1, f._2)
					}.toMap, (_: Any) => 1.0)
			}
		val nonStringMap: Seq[(Int, (Int, (Any => Int), (Any => Double)))] =
			nonStringFields.map { f =>
				(f, (1, (_: Any) => 0, DataTransformer.toDouble _))
			}
		stringFieldsRdd.union(stringFieldsRdd.context.parallelize(nonStringMap))
	}

	def convertRowToDouble(toBeConverted: Row): Row = {
		val values = (0 until toBeConverted.length).map { index =>
			val value = toBeConverted.get(index)
			DataTransformer.toDouble(value)
		}
		Row(values)
	}

	private def summarizeColumnsIndex = {
		val fieldNames =
			this.sqlContext.sparkContext.broadcast(this.schema.fieldNames)
		val summarized = summarizedColumns
			.sortBy(_._1)
			.map { f =>
				f._2._2 match {
					case m: Map[Any, Int] =>
						(f._1,
							m.map(
								value => value._2 -> (fieldNames.value(f._1), value._1.toString)
							))
					case _: (Any => Int) =>
						(f._1, Map(0 -> (fieldNames.value(f._1), "")))
				}
			}
			.collect
		summarized.foldLeft(Map.empty[Int, (String, String)])(
			(b, a) => b ++ a._2.map(f => f._1 + b.size -> f._2)
		)
	}

	type DateSplitterColumnSize = (Long, Long, Int) => Int
	type NoSplitterColumnSize = (Long, Int) => Int
	type RowDateSplitter = (Long, DateTimeToInt, Seq[Int]) => Seq[Int]
	type DateTimeToInt = DateTime => Int
	val dayZero = new DateTime(1970, 1, 1, 0, 0, 0)
	val daysBetween: DateTimeToInt = { d: DateTime =>
		Days.daysBetween(dayZero, d).getDays
	}
	val getDayOfMonth: DateTimeToInt = { d: DateTime =>
		d.getDayOfMonth
	}
	val getMonthOfYear: DateTimeToInt = { d: DateTime =>
		d.getMonthOfYear
	}
	val getYear: DateTimeToInt = { d: DateTime =>
		d.getYear
	}
	val getDayOfAWeek: DateTimeToInt = { d: DateTime =>
		d.getDayOfWeek
	}
	val getPeriod: DateTimeToInt = { d: DateTime =>
		dateTimeParser.period(d).id
	}

	def formatDateValues(columnName: String, dateSplitter: Long): UberDataset =
		formatDateValues(columnIndexOf(columnName), dateSplitter)

	def formatDateValues(index: Int, dateSplitter: Long): UberDataset = {

		val (beforeFields, afterFields) = getDataFrameSchema.toArray.splitAt(index)
		val rdd = dataFrame.rdd.map { f =>
			val (before, after) = f.toSeq.splitAt(index)
			val formattedDate = splitDateValues(f, index, dateSplitter)
			Row(
				before ++ formattedDate ++ after.headOption.map(_ => after.tail).getOrElse(Seq.empty): _*
			)
		}
		val dateFields = (1 to determineSizeOfSplitter(dateSplitter)).map(
			index =>
				StructField(
					afterFields.head.name
						+ index,
					org.apache.spark.sql.types.IntegerType,
					nullable = false
				)
		)
		val fields = beforeFields ++ dateFields ++ afterFields.headOption
			.map(_ => afterFields.tail)
			.getOrElse(Array.empty[StructField])
		val newSchema = StructType(fields)

		// TODO: Does convert need to be called here ?
		val newDataFrame = convert(
			dataFrame.sqlContext.createDataFrame(rdd, newSchema),
			newSchema,
			dateTimeParser
		)

		new UberDataset(newDataFrame, Some(this))
	}

	private def determineSizeOfSplitter(dateSplitter: Long) =
		splitVerifier(
			dateSplitter,
			Year,
			splitVerifier(
				dateSplitter,
				MonthOfYear,
				splitVerifier(
					dateSplitter,
					DayOfMonth,
					splitVerifier(
						dateSplitter,
						Period,
						splitVerifier(dateSplitter, DayOfAWeek, noSplit(dateSplitter, 0))
					)
				)
			)
		)

	private def splitVerifier: DateSplitterColumnSize =
		(dateSplitter: Long, verifier: Long, value: Int) =>
			if (contains(dateSplitter, verifier)) {
				value + 1
			} else value

	private def noSplit: NoSplitterColumnSize =
		(dateSplitter: Long, value: Int) =>
			if (contains(dateSplitter, NoSplit)) {
				0
			} else value

	protected def splitDateValues(line: Row, index: Int, dateSplitter: Long) = {
		def splitDateValues: RowDateSplitter = {
			(verifier: Long, dateTimeFunc: DateTimeToInt, seq: Seq[Int]) =>
				if (contains(dateSplitter, verifier)) {
					val dateValue =
						if (line.isNullAt(index)) dayZero
						else
							new DateTime(
								line(index).asInstanceOf[Timestamp].getTime,
								DateTimeZone.UTC
							)
					seq ++ Seq(dateTimeFunc(dateValue))
				} else seq
		}

		splitDateValues(
			Year,
			getYear,
			splitDateValues(
				MonthOfYear,
				getMonthOfYear,
				splitDateValues(
					DayOfMonth,
					getDayOfMonth,
					splitDateValues(
						Period,
						getPeriod,
						splitDateValues(
							DayOfAWeek,
							getDayOfAWeek,
							splitDateValues(NoSplit, daysBetween, Seq.empty[Int])
						)
					)
				)
			)
		)
	}
}

class FileDataset protected[data](@transient uc: IUberdataContext,
																	file: String,
																	separator: String = ",",
																	header: Option[String] = None,
																	dateTimeParser: DateTimeParser = DateTimeParser()
																	//                                  , schema2: Option[StructType]
																 ) extends Serializable {
	val slf4jLogger: Logger  = LoggerFactory.getLogger(UberDataset.getClass);

	lazy val numberOfPartitions = 4 * ClusterSettings.getNumberOfCores
	lazy val firstLine: String = {
		slf4jLogger.info(s"firstLine: ${loadedRDD.count()}")
		loadedRDD.first
	}
	lazy val columnNames: Array[String] =
		headerOrFirstLine().split(separator, -1)
	lazy val loadedRDD = {
		slf4jLogger.info(s"localFileName:$localFileName")
		uc.sparkSession.sparkContext.textFile(localFileName)
	}
	lazy val localFileName: String = {
		uc.sparkSession.sparkContext
		// make sure that the cluster is up
		val uri = Some(new URI(file))
		val destURI = uri.filter { f =>
			f.getScheme != null && f.getScheme.startsWith("s3")
		}.map { vl =>
			val destURI = s"hdfs:/${ClusterSettings.hdfsHost}//tmp${vl.getPath}"
			try {
				IUberdataIO().copy(file, destURI)
			} catch {
				case e: IOException => e.printStackTrace()
			}
			destURI
		}.getOrElse(file)
		destURI
	}

	lazy val originalRdd: RDD[Array[String]] =
		initOriginalRdd(headerOrFirstLine(), localFileName)
	lazy val dataFrame: DataFrame = if (separator == ",") {
		initCSVDataFrame
	} else {
		initDataFrame(columnNames, originalRdd)
	}

	lazy val firstLineColumnTypes: Array[SqlDataType] = typeLine.map(dataType)
	lazy val typeLine: Array[String] = extractFirstCompleteLine(dataFrame.rdd)

	def headerOrFirstLine(): String = {
		header.getOrElse(firstLine)
	}

	def initOriginalRdd(header: String, localFileName: String): RDD[Array[String]] = {
		initOriginalRdd(header, loadedRDD)
	}

	def initOriginalRdd(header: String, rdd: RDD[String]): RDD[Array[String]] = {
		val localHeader = header
		val oRdd =
			rdd.filter(line => line != localHeader).map(_.split(separator, -1))
		oRdd.setName(localFileName)
		oRdd.cache
	}

	def header(newHeader: String): FileDataset = {
		new FileDataset(uc, file, separator, header = Some(newHeader))
	}

	def toDataFrame: DataFrame = convert(dataFrame, schema, dateTimeParser)

	def toDataset: UberDataset = {
		new UberDataset(dataFrame)
	}

	protected def initCSVDataFrame: DataFrame = {
		val df = uc.sqlContext.read
			.option("header", "true")
			.csv(localFileName)
		df.na.fill("")
	}

	protected def initDataFrame(columnNames: Array[String],
															originalRdd: RDD[Array[String]]): DataFrame = {
		val sqlContext = uc.sqlContext
		val colNames = columnNames
		val types = StructType(
			colNames.map(name => StructField(name, StringType)).toSeq
		)
		val rowRdd = originalRdd.map { colValues =>
			if (colValues.length != colNames.length)
				throw new UnexpectedFileFormatException(
					s"Files should have the same number " +
						s"of columns. Line ${colValues.mkString(",")} \n has #${colValues.length} and Header have " +
						s"#${colNames.length}"
				)
			Row(colValues: _*)
		}
		val dataFrame = sqlContext.createDataFrame(rowRdd, types)
		val tableName = extractTableName(file)
		dataFrame.createOrReplaceTempView(tableName)
		dataFrame.repartition(numberOfPartitions)
		dataFrame
	}

	protected def extractTableName(file: String): String = {
		val name = file.split("/").last
		val index = name.indexOf(".csv") + name.indexOf(".txt")
		name.splitAt(index + 1).productIterator.toList.filter(!_.toString.isEmpty).head.toString
	}

	protected def extractFirstCompleteLine(dataRdd: RDD[Row]): Array[String] = {
		slf4jLogger.info(s"extractFirstCompleteLine ${dataRdd.first()}")
		val df = dataRdd.filter { value =>
			val data = value.toSeq
			!data.contains(null) && {
				val f = value.toSeq.map(_.toString)
				f.nonEmpty &&
					f.forall(!_.isEmpty)
			}
		}
		if (df.count == 0) {
			dataRdd.first().toSeq.map(_.toString).toArray
		} else {
			df.first().toSeq.map(_.toString).toArray
		}
	}

	implicit class Regex(sc: StringContext) {
		def r =
			new scala.util.matching.Regex(
				sc.parts.mkString,
				sc.parts.tail.map(_ => "x"): _*
			)
	}

	private def dataType(data: String): SqlDataType = {
		import org.apache.spark.sql.types._
		data match {
			case r"""-?\d{9,18}""" => LongType
			case r"""-?\d{1,8}""" =>
				LongType // TODO: To return IntType for ints the whole data set (or sample) needs to be analyzed.
			case r"""[+-]?\d*\.?\d*E?\d{1,4}""" if ClusterSettings.enforceDoubleAsBigDecimal =>
				DecimalType(
					ClusterSettings.defaultDecimalPrecision,
					ClusterSettings.defaultDecimalScale
				)
			case r"""[+-]?\d*\.?\d*E?\d{1,4}""" => DoubleType
			case _ => parse(data).getOrElse(StringType)
		}
	}

	protected def parse(data: String): Option[SqlDataType] = {
		import org.apache.spark.sql.types._
		dateTimeParser.isValidDate(data) match {
			case true => Some(TimestampType)
			case false => None
		}
	}

	def applyColumnNames(columnNames: Seq[String]) = {
		require(columnNames.nonEmpty)
		schema = StructType(columnNames zip schema map {
			case (name, field) =>
				StructField(name, field.dataType, field.nullable, field.metadata)
		})
		//    val filteredDataFrame = dataFrame.select(columnNames.map(dataFrame(_)): _*)
		//    new Dataset(filteredDataFrame, Some(this))
	}

	def applyColumnTypes(columnTypes: Seq[SqlDataType]): Unit = {
		val fields = schema.fields.zip(columnTypes).map {
			case (structField, dataType) =>
				StructField(structField.name, dataType)
		}
		//    val newRowRDD = if (structFieldNames.size > 1) dataFrame.select(structFieldNames.head, structFieldNames.tail: _*)
		//    else dataFrame.select(structFieldNames.head)
		//    val newSchemaRDD = convert(newRowRDD, StructType(fields))
		//    new Dataset(newSchemaRDD, Some(this))
		//TODO try to avoid this var
		schema = StructType(fields)
	}

	def applyColumnTypes(
												columnReplacementTypes: Map[String, SqlDataType]
											): Unit = {
		val newDataTypes = schema.fields.map { structField =>
			columnReplacementTypes.getOrElse(structField.name, structField.dataType)
		}.toSeq
		applyColumnTypes(newDataTypes)
	}

	protected def structType: StructType = {
		if (columnNames.length != typeLine.length || columnNames.isEmpty)
			StructType(List.empty[StructField])
		else {
			val fields = columnNames.zip(firstLineColumnTypes).map {
				case (columnName, columnType) =>
					StructField(columnName, columnType, nullable = true)
			}
			StructType(fields)
		}
	}

	private var schema = structType
	lazy val columnIndexOf = this.schema.fieldNames.zipWithIndex.toSet.toMap
}

package object dataset {
	def convert(dataFrame: DataFrame,
							newSchema: StructType,
							dateTimeParser: DateTimeParser): DataFrame = {
		import org.apache.spark.sql.types._

		val converted = dataFrame.rdd.map { row =>
			val values = row.toSeq.zip(newSchema.fields).map {
				case (null, _) => null
				case (s: String, tp: StructField) if s.isEmpty && !tp.dataType.isInstanceOf[StringType] =>
					null
				case (value: Double, StructField(_, DoubleType, _, _)) => value
				case (value: Float, StructField(_, FloatType, _, _)) => value
				case (value: BigDecimal, StructField(_, DecimalType(), _, _)) => value
				case (value: Timestamp, StructField(_, TimestampType, _, _)) => value
				case (value: Long, StructField(_, LongType, _, _)) => value
				case (value: Int, StructField(_, IntegerType, _, _)) => value
				case (value: Short, StructField(_, ShortType, _, _)) => value
				case (value: Boolean, StructField(_, BooleanType, _, _)) => value
				case (value: Byte, StructField(_, ByteType, _, _)) => value
				case (value: Array[Byte], StructField(_, StringType, _, _)) =>
					new String(value)
				case (value, StructField(_, DecimalType(), _, _)) =>
					BigDecimal(value.toString)
				case (value, StructField(_, FloatType, _, _)) => value.toString.toFloat
				case (value: Boolean, StructField(_, DoubleType, _, _)) =>
					if (value) 1d else 0d
				case (value, StructField(_, DoubleType, _, _)) =>
					value.toString.toDouble
				case (value: Timestamp, StructField(_, LongType, _, _)) =>
					value.getTime
				case (value, StructField(_, LongType, _, _)) => value.toString.toLong
				case (value, StructField(_, IntegerType, _, _)) => value.toString.toInt
				case (value, StructField(_, ShortType, _, _)) => value.toString.toShort
				case (value, StructField(_, ByteType, _, _)) => value.toString.toByte
				//convert from double
				case (value, StructField(_, BooleanType, _, _)) =>
					value.toString match {
						case "1" | "t" | "true" => true
						case "0" | "f" | "false" => false
						case a =>
							throw new InvalidDataException(s"$a is an invalid Boolean value")
					}
				case (value, StructField(_, TimestampType, _, _)) =>
					new Timestamp(
						dateTimeParser
							.parse(value.toString)
							.map(_.toDate.getTime)
							.getOrElse(
								throw new InvalidDataException(
									"Unsupported data format Exception, please specify the date format"
								)
							)
					)
				case (value, StructField(_, StringType, _, _)) =>
					value.toString.stripMargin('"').stripSuffix(""""""")
			}
			Row(values: _*)
		}
		dataFrame.sqlContext.createDataFrame(converted, newSchema)
	}
}
