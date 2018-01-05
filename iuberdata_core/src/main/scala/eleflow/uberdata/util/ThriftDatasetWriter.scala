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


import java.sql.{Connection, DriverManager, Statement}

import scala.reflect.runtime.universe._
import org.apache.spark.sql.Dataset
import io.circe.syntax._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.hcatalog.streaming.ThriftDatasetWriter.dbName
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

		writeSingle(toBeWriten, broadTableName, broadDbName, broadMetastore, broadColumns, broadMaxBatchGroups)


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
					val partitions = list.map(_._1).filter(_.nonEmpty)
					val partitionNames = partitions.flatMap(_ => partitions.map(_.substring(0, 6))).distinct
					logger.warn(s"Partitions: ${partitionNames.mkString}")
					logger.warn(s"TableName: ${broadTableName.value}")
					logger.warn(s"DataSize: ${data.size}")

					partitionNames.foreach {
						partitions =>
							writeToPartitionedTable(broadMetastore.value, broadDbName.value, broadTableName.value,
								List(partitions), broadColumns.value, broadMaxBatchGroups.value, data, maxSize)
					}
					partitionNames.headOption.getOrElse {
						writeToPartitionedTable(broadMetastore.value, broadDbName.value, broadTableName.value,
							List.empty[String], broadColumns.value, broadMaxBatchGroups.value, data, maxSize)
					}
				}
		}
	}

	private lazy val driverName = "org.apache.hive.jdbc.HiveDriver"

	def assemblySelectQuery(dbName: String, tableName: String, columns: Seq[String],
													whereClause: Option[String] = None) =
		s"select ${columns.mkString(",")} from $dbName.$tableName ${
			whereClause.map { where => s"where $where" }.getOrElse("")
		}"

	def select(serverAddress: String, dbName: String, tableName: String, user: String,
						 password: String, columns: Seq[String] = Seq(" * "), whereClause: Option[String] =
						 None): 	List[Seq[String]] = {
		try {
			val func: Statement => List[Seq[String]] = { stmt =>
				val query = assemblySelectQuery(dbName, tableName, columns, whereClause)
				val resultSet = stmt.executeQuery(query)
				val metadata = resultSet.getMetaData
				val sz = metadata.getColumnCount
				val result = new Iterator[Seq[String]] {
					def hasNext = resultSet.next()

					def next = (1 to sz).map(resultSet.getString)
				}.toList
				resultSet.close()
				result
			}
			createConnection[List[Seq[String]]](serverAddress, dbName, user, password, func)
		} catch {
			case e: Exception =>
				e.printStackTrace()
				throw e
		}
	}

	def delete(serverAddress: String, dbName: String, tableName: String, user: String,
						 password: String, whereClause: Option[String] = None): Unit = {
		try {
			val func: Statement => Unit = { stmt =>
				val query = s"delete from $dbName.$tableName ${whereClause.map(w => s"where $w")
					.getOrElse("")}"
				stmt.executeUpdate(query)
			}
			createConnection[Unit](serverAddress, dbName, user, password, func)
		} catch {
			case e: Exception =>
				e.printStackTrace()
				throw e
		}
	}

	def insertTable(serverAddress: String, dbName: String, tableName: String, user: String,
									password: String, columns: Array[String], values: Array[String]): Unit = {
		try {
			val func: Statement => Unit = { stmt =>
				stmt.execute(s"insert into $tableName ${buildInsertValues(columns, values)}")
				Unit
			}
			createConnection[Unit](serverAddress, dbName, user, password, func)
		} catch {
			case e: Exception =>
				e.printStackTrace()
				throw e
		}
	}

	private def buildInsertValues(columns: Array[String], values: Array[String]): String =
		s"(${columns.mkString(",")}) values('${values.mkString("','")}')"


	def updateTable(serverAddress: String, dbName: String, tableName: String, user: String,
									password: String, columns: Array[String], values: Array[String],
									whereclause: String): Unit = {
		try {
			val func: Statement => Unit = { stmt =>
				stmt.execute(s"update $tableName set ${removeLastComma(buildUpdateValues(columns, values))}"
					+ s" where $whereclause")
				Unit
			}
			createConnection[Unit](serverAddress, dbName, user, password, func)
		} catch {
			case e: Exception =>
				e.printStackTrace()
				throw e
		}
	}

	private def createStatement[T](connection: Connection, stmtFunc: Statement => T): T = {
		val stmt: Statement = connection.createStatement()
		try {
			stmtFunc.apply(stmt)
		} finally {
			stmt.close()
		}
	}

	private var password = ""
	private var user = "uberdata"
	private var dbName = "default"
	private var serverAddress: String = "localhost:10500"
	private var con: Connection = null

	private def createConnection[T](serverAddress: String, dbName: String, user: String,
																	password: String, func: Statement => T): T = {
		Class.forName(driverName)
		this.serverAddress = serverAddress
		this.user = user
		this.dbName = dbName
		this.password = password
		createConnection
		createStatement[T](con, func)
	}

	private def createConnection = {
		if (con == null || con.isClosed) {
			logger.warn(s"serverAddress $serverAddress")
			logger.warn(s"DBName $dbName")
			con = DriverManager.getConnection(s"$serverAddress/$dbName", user,
				password)
		}
	}

	def closeConnection = {
		try {
			if (!con.isClosed()) con.close
		} catch {
			case e: Exception => e.printStackTrace
		}
	}

	private def removeLastComma(updateClause: String) =
		updateClause.substring(0, updateClause.lastIndexOf(','))


	private def buildUpdateValues(columns: Array[String], values: Array[String]): String =
		columns.zip(values).foldLeft("") {
			case (result, (column, value)) => result + s"$column = ${processNullValue(value)}, "
		}

	private def processNullValue(value: String) = if (value == null) "null" else s"'$value'"


	private def writeToPartitionedTable(metastore: String, dbName: String, tableName: String,
																			partitions: List[String], columns: Array[String],
																			maxBatchGroups: Int, data: List[String], maxSize: Int)
	= {

		val endPt = new HiveEndPoint(metastore, dbName, tableName, partitions)
		val conf: HiveConf = null
		val connection = endPt.newConnection(true, conf)
		val writer = columns.headOption.map {
			_ => new DelimitedInputWriter(columns, ",", endPt);
		}.getOrElse(new StrictJsonWriter(endPt))
		val txnBatch = connection.fetchTransactionBatch(maxBatchGroups,
			writer)
		try {
			writeData(data, maxSize, txnBatch, writer, connection, maxBatchGroups,
				columns, tableName)
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