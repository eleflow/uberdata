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

package eleflow.uberdata.core.json

import java.util.Properties

import eleflow.uberdata.core.data.json.{TaskEnd, _}
import org.apache.spark.UberRDDOperationScope
import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.storage.{BlockId, BlockStatus, RDDInfo, StorageLevel}
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
  * Created by dirceu on 04/12/15.
  */
object SparkJsonMapper {

  implicit val executorAddedWrites = Json.writes[ExecutorAdded]
  implicit val executorAddedFormat = Json.format[ExecutorAdded]

  implicit val executorRemovedWrites = Json.writes[ExecutorRemoved]
  implicit val executorRemovedFormat = Json.format[ExecutorRemoved]

  implicit val executorMetricsUpdateWrites =
    Json.writes[ExecutorMetricsUpdated]
  implicit val executorMetricsUpdateFormat =
    Json.format[ExecutorMetricsUpdated]

  implicit val storageLvlWrites = Json.writes[StorageLvl]
  implicit val storageLvlFormat = Json.format[StorageLvl]

  implicit val rddInfoWrites = Json.writes[UberRDDInfo]
  implicit val rddInfoFormat = Json.format[UberRDDInfo]

  implicit val accumulableInfoWrites = Json.writes[AccumulableInfo]
  implicit val accumulableInfoFormat = Json.format[AccumulableInfo]

  implicit val stageWrites = Json.writes[Stage]
  implicit val stageFormat = Json.format[Stage]

  implicit val stageJobRelationWrites = Json.writes[StageJobRelation]
  implicit val stageJobRelationFormat = Json.format[StageJobRelation]

  implicit val taskStartWrites = Json.writes[TaskStart]
  implicit val taskStartFormat = Json.format[TaskStart]

  implicit val blockUpdatedWrites = Json.writes[BlockUpdated]
  implicit val blockUpdatedFormat = Json.format[BlockUpdated]

  implicit val blockMetricsWrites = Json.writes[BlockMetrics]
  implicit val blockMetricsFormat = Json.format[BlockMetrics]

  implicit val blockStatusWrite = Json.writes[UberBlockStatus]
  implicit val blockStatusFormat = Json.format[UberBlockStatus]

  implicit val blockIdWrite = Json.writes[UberBlockId]
  implicit val blockIdFormat = Json.format[UberBlockId]

  implicit val taskEndCompWrites: Writes[TaskEndComp] =
    Json.writes[TaskEndComp]

  implicit val taskEndCompFormat: Format[TaskEndComp] =
    Json.format[TaskEndComp]

  implicit val taskEndWrites = new Writes[TaskEnd] {
    def writes(task: TaskEnd) = Json.obj(
      "appId" -> task.appId,
      "stageId" -> task.stageId.toString,
      "stageAttemptId" -> task.stageAttemptId.toString,
      "taskType" -> task.taskType,
      "taskId" -> task.taskId.toString,
      "taskEndReason" -> task.taskEndReason,
      "index" -> task.index.toString,
      "attemptNumber" -> task.attemptNumber.toString,
      "launchTime" -> task.launchTime.toString,
      "executorId" -> task.executorId,
      "host" -> task.host,
      "taskLocality" -> task.taskLocality,
      "speculative" -> task.speculative.toString,
      "executorDeserializeTime" -> task.executorDeserializeTime.toString,
      "executorRunTime" -> task.executorRunTime.toString,
      "resultSize" -> task.resultSize.toString,
      "jvmGCTime" -> task.jvmGCTime.toString,
      "resultSerializationTime" -> task.resultSerializationTime.toString,
      "memoryBytesSpilled" -> task.memoryBytesSpilled.toString,
      "diskBytesSpilled" -> task.diskBytesSpilled.toString,
      "bytesRead" -> task.bytesRead.map(_.toString),
      "recordsRead" -> task.recordsRead.map(_.toString),
      "bytesWritten" -> task.bytesWritten.map(_.toString),
      "recordsWritten" -> task.recordsWritten.map(_.toString),
      "remoteBlocksFetched" -> task.remoteBlocksFetched.map(_.toString),
      "localBlocksFetched" -> task.localBlocksFetched.map(_.toString),
      "fetchWaitTime" -> task.fetchWaitTime.map(_.toString),
      "remoteBytesRead" -> task.remoteBytesRead.map(_.toString),
      "localBytesRead" -> task.localBytesRead.map(_.toString),
      "totalBytesRead" -> task.totalBytesRead.map(_.toString),
      "totalBlocksFetched" -> task.totalBlocksFetched.map(_.toString),
      "shuffleRecordsRead" -> task.shuffleRecordsRead.map(_.toString),
      "shuffleBytesWritten" -> task.shuffleBytesWritten.map(_.toString),
      "shuffleWriteTime" -> task.shuffleWriteTime.map(_.toString),
      "shuffleRecordsWritten" -> task.shuffleRecordsWritten.map(_.toString)/*,
      "updatedBlocks" -> task.updatedBlocks*/
    )
  }

  implicit val taskGettingResultWrites = Json.writes[TaskGettingResult]
  implicit val taskGettingResultFormat = Json.format[TaskGettingResult]

  implicit val jobStartWrites = Json.writes[JobStart]
  implicit val jobStartFormat = Json.format[JobStart]

  implicit val jobEndWrites = Json.writes[JobEnd]
  implicit val jobEndFormat = Json.format[JobEnd]

  implicit val applicationEndWrites = Json.writes[SparkListenerApplicationEnd]
  implicit val applicationEndFormat = Json.format[SparkListenerApplicationEnd]

  implicit val blockManagerRemovedWrites = Json.writes[BlockManagerRemoved]
  implicit val blockManagerRemovedFormat = Json.format[BlockManagerRemoved]
  implicit val blockManagerAddedWrites = Json.writes[BlockManagerAdded]
  implicit val blockManagerAddedFormat = Json.format[BlockManagerAdded]

  implicit val keyValueWrites = Json.writes[KeyValue]
  implicit val keyValueFormat = Json.format[KeyValue]

  implicit val keyValueListWrites = Json.writes[KeyValueList]
  implicit val keyValueListFormat = Json.format[KeyValueList]

  implicit val applicationStartWrites = Json.writes[SparkApplicationStart]
  implicit val applicationStartFormat = Json.format[SparkApplicationStart]
}
