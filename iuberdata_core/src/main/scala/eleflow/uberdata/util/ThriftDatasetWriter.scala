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
package eleflow.uberdata.util


import scala.reflect.runtime.universe._
import org.apache.hive.hcatalog.streaming._
import org.apache.spark.sql.Dataset
import io.circe.syntax._
import org.slf4j.LoggerFactory


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

	def writeToHiveStream[T](united: Dataset[T], metastore: String = null,
													 dbName: String = "default", tableName: String = "cdrs",
													 maxBatchGroups: Int = 1000): Unit = {
		val context = united.sparkSession.sparkContext
		val broadDbName = context.broadcast(dbName)
		val broadTableName = context.broadcast(tableName)
		val broadMetastore = context.broadcast(metastore)
		val broadMaxBatchGroups = context.broadcast(maxBatchGroups)
		united.toJSON.foreachPartition {
			data =>
				val maxSize = 1000
				val endPt = new HiveEndPoint(
					broadMetastore.value,
					broadDbName.value, broadTableName.value, null)
				val writer = new StrictJsonWriter(endPt)
				val connection = endPt.newConnection(false)
				val txnBatch = connection.fetchTransactionBatch(broadMaxBatchGroups.value,
					writer)
				txnBatch.beginNextTransaction()
				writeData(data.toList, maxSize, txnBatch, writer, connection, broadMaxBatchGroups.value)
				txnBatch.close()
		}
	}

	private def writeStream(data: String, txnBatch: TransactionBatch): Unit = {
		try {
			val jsonString = data.asJson.toString
			val outputString = jsonString.substring(1, jsonString.size - 1).replaceAll("""\\"""", """"""")
			txnBatch.write(outputString.getBytes())
		} catch {
			case (e: Exception) =>
				e.printStackTrace()
		}
	}

	private def getNextTransaction(txnBatch: TransactionBatch,
																 writer: StrictJsonWriter, connection: StreamingConnection,
																 maxBatchGroups: Int) =
		if (txnBatch.remainingTransactions() > 0) {
			logger.warn(s"->  txnBatch transactions remaining: ${txnBatch.remainingTransactions()}")
			txnBatch
		} else {
			logger.warn("->  Refereshing the transaction group count")
			connection.fetchTransactionBatch(maxBatchGroups, writer)
		}

	def writeData(data: List[String], maxSize: Int, txnBatch: TransactionBatch,
								writer: StrictJsonWriter, connection: StreamingConnection, maxBatchGroups: Int):
	Unit	= {
		logger.warn(s"datasize ${data.size} maxSize = $maxSize")
		if (data.size > maxSize) {
			val txn = getNextTransaction(txnBatch, writer, connection, maxBatchGroups)
			writeData(data.take(maxSize), txn, writer, connection)
			writeData(data.drop(maxSize), maxSize, txn, writer, connection, maxBatchGroups)
			logger.warn(s"-> Begining Transaction Commit: Transaction State: " +
				s"${txn.getCurrentTransactionState()}")
		} else {
			val txn = getNextTransaction(txnBatch, writer, connection, maxBatchGroups)
			writeData(data, txn, writer, connection)
		}
		logger.warn("commit")
	}

	private def writeData(data: List[String], txnBatch: TransactionBatch,
												writer: StrictJsonWriter, connection: StreamingConnection) = {
		txnBatch.beginNextTransaction()
		data.foreach { dt =>
			writeStream(dt, txnBatch)
		}
		writer.flush()
		txnBatch.commit()
	}
}