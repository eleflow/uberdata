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
package org.apache.hive.hcatalog.streaming


import scala.reflect.runtime.universe._
import org.apache.spark.sql.Dataset
import io.circe.syntax._
import org.apache.hive.hcatalog.streaming.TransactionBatch.TxnState
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.matching.Regex


object ThriftDatasetWriter {

	val logger = LoggerFactory.getLogger(ThriftDatasetWriter.getClass)

	def allFields[T: TypeTag]: Array[String] = {
		def rec(tpe: Type): List[List[String]] = {
			val collected = tpe.members.collect {
				case m: MethodSymbol if m.isCaseAccessor => m
			}.toList
			if (collected.nonEmpty) {
				collected.flatMap(m => rec(m.returnType.typeArgs.head).map(m.name.toString :: _))
			} else {
				List(Nil)
			}
		}

		rec(typeOf[T]).withFilter(_.nonEmpty).map(_.mkString(".")).toArray
	}

	def writeClassToHiveStream(toBeWriten: Dataset[(String, String)], metastore: String,
														 dbName: String, tableName: String,
														 maxBatchGroups: Int, columns: Array[String]): Unit = {
		writeData(toBeWriten, metastore, dbName, tableName, maxBatchGroups, columns)
	}


	def writeClassToHiveStream(toBeWriten: Dataset[(String, String)],
														 metastore: String,
														 dbName: String, tableName: String,
														 maxBatchGroups: Int): Unit = {
		writeData(toBeWriten, metastore, dbName, tableName,
			maxBatchGroups)
	}

	private def writeData(toBeWriten: Dataset[(String, String)], metastore: String,
												dbName: String, tableName: String,
												maxBatchGroups: Int, columns: Array[String] = Array.empty[String],
												pattern: Option[Regex] = None): Unit = {
		val context = toBeWriten.sparkSession.sparkContext
		val broadDbName = context.broadcast(dbName)
		val broadTableName = context.broadcast(tableName)
		val broadMetastore = context.broadcast(metastore)
		val broadMaxBatchGroups = context.broadcast(maxBatchGroups)
		val broadColumns = context.broadcast(columns)

		val tobe = if (toBeWriten.count > 0) {
			val first = toBeWriten.first()
			writeSingle(toBeWriten, broadTableName, broadDbName, broadMetastore, broadColumns, broadMaxBatchGroups)
		} else writeSingle(toBeWriten, broadTableName, broadDbName, broadMetastore, broadColumns,
			broadMaxBatchGroups)


	}

	private def writeSingle(tobe: Dataset[(String, String)], broadTableName: Broadcast[String],
													broadDbName: Broadcast[String], broadMetastore: Broadcast[String],
													broadColumns: Broadcast[Array[String]], broadMaxBatchGroups:
													Broadcast[Int]) = {
		tobe.foreachPartition {
			dataPartition =>

				val list = dataPartition.toList
				if (list.nonEmpty) {
					val maxSize = Int.MaxValue
					val data = list.map(_._2)
					val partitionNames = list.map(_._1).filter(_.nonEmpty).map(_.substring(0, 6)).distinct
					logger.warn(s"Partitions: ${partitionNames.mkString}")
					logger.warn(s"TableName: ${broadTableName.value}")
					partitionNames.map {
						partitions =>
						val endPt = new HiveEndPoint(
							broadMetastore.value, broadDbName.value, broadTableName.value, List(partitions))
						val connection = endPt.newConnection(true, s"${broadTableName.value}:" +
							s"${partitions.mkString}")
						val writer = broadColumns.value.headOption.map {
							_ => new DelimitedInputWriter(broadColumns.value, ",", endPt);
						}.getOrElse(new StrictJsonWriter(endPt))
						val txnBatch = connection.fetchTransactionBatch(broadMaxBatchGroups.value,
							writer)
						try {
							writeData(data, maxSize, txnBatch, writer, connection, broadMaxBatchGroups.value,
								broadColumns.value, broadTableName.value)
						} catch {
							case e: Exception =>
								e.printStackTrace()
								txnBatch.abort()
								throw e
						} finally {
							logger.warn(txnBatch.getCurrentTransactionState.toString)
							if (txnBatch.getCurrentTransactionState == TxnState.OPEN) {
								//							writer.flush()
								txnBatch.commit()
							}
							txnBatch.close()
							connection.close()
						}
					}
				}
		}
	}

	private def writeXMLStream(data: String, txnBatch: TransactionBatch): Unit = {
		val outputString = data.replaceAll("""\\"""", """"""")
		txnBatch.write(outputString.getBytes())
	}


	private def writeJSONStream(data: String, txnBatch: TransactionBatch): Unit = {
		txnBatch.write(formatJson(data).getBytes())
	}

	private def formatJson(string: String) = {
		val json = string.asJson.noSpaces.substring(1)
		json.replaceAll("""\\"""", """"""").replaceAll("""\\"""", """"""")
	}

	private def getNextTransaction(txnBatch: TransactionBatch,
																 writer: AbstractRecordWriter, connection: StreamingConnection,
																 maxBatchGroups: Int) =
		if (txnBatch.remainingTransactions() > 0) {
			logger.warn(s"->  txnBatch transactions remaining: ${txnBatch.remainingTransactions()}")
			txnBatch
		} else {
			logger.warn("->  Refereshing the transaction group count")
			connection.fetchTransactionBatch(maxBatchGroups, writer)
		}

	def writeData(data: List[String], maxSize: Int, txnBatch: TransactionBatch,
								writer: AbstractRecordWriter, connection: StreamingConnection, maxBatchGroups: Int,
								columns: Array[String], tableName: String): Unit = {
		logger.warn(s"Tablename: $tableName")
		logger.warn(s"datasize ${data.size} maxSize = $maxSize")
		if (data.size > maxSize) {
			val txn = getNextTransaction(txnBatch, writer, connection, maxBatchGroups)
			writeData(data.take(maxSize), txn, writer, connection, columns)
			writeData(data.drop(maxSize), maxSize, txn, writer, connection, maxBatchGroups, columns,
				tableName)
			logger.warn(s"-> Begining Transaction Commit: Transaction State: " +
				s"${txn.getCurrentTransactionState()}")
		} else if (data.size > 0) {
			val txn = getNextTransaction(txnBatch, writer, connection, maxBatchGroups)
			logger.warn(s"-> Begining Transaction Commit: Transaction State: " +
				s"${txn.getCurrentTransactionState()}")
			writeData(data, txn, writer, connection, columns)
		}
	}

	private def writeData(data: List[String], txnBatch: TransactionBatch,
												writer: AbstractRecordWriter, connection: StreamingConnection,
												columns: Array[String]) = {
		try {
			txnBatch.beginNextTransaction()
			data.filterNot(_.isEmpty).foreach { dt =>
				writer match {
					case _: StrictJsonWriter =>
						writeJSONStream(dt, txnBatch)
					case _: DelimitedInputWriter =>
						writeXMLStream(dt, txnBatch)
				}
			}
			writer.flush()
			txnBatch.commit()
		} catch {
			case e: Exception =>
				e.printStackTrace()
				txnBatch.abort()
				throw e
		}
	}
}

case class Contents(key: String, value: String)