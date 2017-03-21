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

package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 28/12/15.
  */
case class ExecutorAdded(initTimestamp: Long,
                         executorId: String,
                         time: Long,
                         masterHost: String,
                         executorHost: String,
                         totalCores: Int,
                         logUrlMap: Map[String, String],
                         cacheMemory: Long,
                         remainingMemory: Long,
                         executorMemory: Long)

case class ExecutorRemoved(initTimestamp: Long, executorId: String, time: Long, reason: String)

case class ExecutorMetricsUpdated(executorId: String,
                                  time: Long,
                                  taskId: Long,
                                  stageId: Int,
                                  bytesRead: Option[Long] = None,
                                  recordsRead: Option[Long] = None,
                                  writeMethod: Option[String],
                                  bytesWritten: Option[Long] = None,
                                  recordsWritten: Option[Long] = None,
                                  remoteBlocksFetched: Option[Long] = None,
                                  localBlocksFetched: Option[Long] = None,
                                  fetchWaitTime: Option[Long] = None,
                                  remoteBytesRead: Option[Long] = None,
                                  localBytesRead: Option[Long] = None,
                                  totalBytesRead: Option[Long] = None,
                                  totalBlocksFetched: Option[Long] = None,
                                  shuffleRecordsRead: Option[Long] = None,
                                  shuffleBytesWritten: Option[Long] = None,
                                  shuffleWriteTime: Option[Long] = None,
                                  shuffleRecordsWritten: Option[Long] = None)


//(taskId, stageId, stageAttemptId, accumUpdates)
//accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])])


//case class SparkListenerExecutorMetricsUpdate(
//    execId: String,
//    accumUpdates: Seq[(Long, Int, Int, Seq[String])])

case class AccumulatorInfoUpdateEvent(executorId: String,
                                      accumUpdates: Seq[(Long, Int, Int, Seq[String])]
                                  /*taskId: Long,
                                  stageId: Int,
                                  stageAttemptId: Int,
                                  idAccumulableInfo: Long,
                                  nameAccumulableInfo: Option[String],
                                  updateAccumulableInfo: Option[Any],
                                  valueAccumulableInfo: Option[Any]*/
                                )

case class Workers(id: String,
                   host: String,
                   port: Int,
                   webuiaddress: String,
                   cores: Int,
                   coresused: Int,
                   coresfree: Int,
                   memory: Long,
                   memoryused: Long,
                   memoryfree: Long,
                   state: String,
                   lastheartbeat: Long)
