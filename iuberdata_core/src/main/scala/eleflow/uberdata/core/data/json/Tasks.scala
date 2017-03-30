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
  * Created by dirceu on 30/11/15.
  */
case class TaskStart(appId: String,
                     stageId: Int,
                     stageAttemptId: Int,
                     taskId: Long,
                     index: Int,
                     attemptNumber: Int,
                     launchTime: Long,
                     executorId: String,
                     host: String,
                     taskLocality: String,
                     speculative: Boolean) {}

case class TaskGettingResult(appId: String,
                             taskId: Long,
                             index: Int,
                             attemptNumber: Int,
                             launchTime: Long,
                             executorId: String,
                             host: String,
                             taskLocality: String,
                             speculative: Boolean) {}

case class UberBlockId(name: String, status: UberBlockStatus)

class TaskEnd(val appId: String,
              val stageId: Int,
              val stageAttemptId: Int,
              val taskType: String,
              val taskId: Long,
              val taskEndReason: String,
              val index: Int,
              val attemptNumber: Int,
              val launchTime: Long,
              val executorId: String,
              val host: String,
              val taskLocality: String,
              val speculative: Boolean,
              val executorDeserializeTime: Long,
              val executorRunTime: Long,
              val resultSize: Long,
              val jvmGCTime: Long,
              val resultSerializationTime: Long,
              val memoryBytesSpilled: Long,
              val diskBytesSpilled: Long,
              val bytesRead: Option[Long] = None,
              val recordsRead: Option[Long] = None,
              val bytesWritten: Option[Long] = None,
              val recordsWritten: Option[Long] = None,
              val remoteBlocksFetched: Option[Long] = None,
              val localBlocksFetched: Option[Long] = None,
              val fetchWaitTime: Option[Long] = None,
              val remoteBytesRead: Option[Long] = None,
              val localBytesRead: Option[Long] = None,
              val totalBytesRead: Option[Long] = None,
              val totalBlocksFetched: Option[Long] = None,
              val shuffleRecordsRead: Option[Long] = None,
              val shuffleBytesWritten: Option[Long] = None,
              val shuffleWriteTime: Option[Long] = None,
              val shuffleRecordsWritten: Option[Long] = None,
              val updatedBlocks: Seq[UberBlockId])
    extends Mappable
    with Product
    with Serializable {
  def canEqual(that: Any) = that.isInstanceOf[TaskEnd]

  def productArity = 39 // number of columns

  def productElement(idx: Int) = idx match {
    case 0 => appId
    case 1 => stageId
    case 2 => stageAttemptId
    case 3 => taskType
    case 4 => taskId
    case 5 => taskEndReason
    case 6 => index
    case 7 => attemptNumber
    case 8 => launchTime
    case 9 => executorId
    case 10 => host
    case 11 => taskLocality
    case 12 => speculative
    case 13 => executorDeserializeTime
    case 14 => executorRunTime
    case 15 => resultSize
    case 16 => jvmGCTime
    case 17 => resultSerializationTime
    case 18 => memoryBytesSpilled
    case 19 => diskBytesSpilled
    case 20 => bytesRead
    case 21 => recordsRead
    case 22 => bytesWritten
    case 23 => recordsWritten
    case 24 => remoteBlocksFetched
    case 25 => localBlocksFetched
    case 26 => fetchWaitTime
    case 27 => remoteBytesRead
    case 28 => localBytesRead
    case 29 => totalBytesRead
    case 30 => totalBlocksFetched
    case 31 => shuffleRecordsRead
    case 32 => shuffleBytesWritten
    case 33 => shuffleWriteTime
    case 34 => shuffleRecordsWritten
    case 35 => updatedBlocks
  }

  def apply(appId: String,
            stageId: Int,
            stageAttemptId: Int,
            taskType: String,
            taskId: Long,
            taskEndReason: String,
            index: Int,
            attemptNumber: Int,
            launchTime: Long,
            executorId: String,
            host: String,
            taskLocality: String,
            speculative: Boolean,
            executorDeserializeTime: Long,
            executorRunTime: Long,
            resultSize: Long,
            jvmGCTime: Long,
            resultSerializationTime: Long,
            memoryBytesSpilled: Long,
            diskBytesSpilled: Long,
            bytesRead: Option[Long] = None,
            recordsRead: Option[Long] = None,
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
            shuffleRecordsWritten: Option[Long] = None ,
            updatedBlocks: Seq[UberBlockId]) =
    new TaskEnd(
      appId,
      stageId,
      stageAttemptId,
      taskType,
      taskId,
      taskEndReason,
      index,
      attemptNumber,
      launchTime,
      executorId,
      host,
      taskLocality,
      speculative,
      executorDeserializeTime,
      executorRunTime,
      resultSize,
      jvmGCTime,
      resultSerializationTime,
      memoryBytesSpilled,
      diskBytesSpilled,
      bytesRead,
      recordsRead,
      bytesWritten,
      recordsWritten,
      remoteBlocksFetched,
      localBlocksFetched,
      fetchWaitTime,
      remoteBytesRead,
      localBytesRead,
      totalBytesRead,
      totalBlocksFetched,
      shuffleRecordsRead,
      shuffleBytesWritten,
      shuffleWriteTime,
      shuffleRecordsWritten,
      updatedBlocks
    )

}

case class TaskEndComp(memoryBytesSpilled: Long,
                       diskBytesSpilled: Long,
                       readMethod: Option[String] = None,
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
                       shuffleRecordsWritten: Option[Long] = None,
                       updatedBlocks: Seq[UberBlockId])
