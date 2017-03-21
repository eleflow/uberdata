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

package org.apache.spark.mllib

import java.util.{Properties, UUID}

import org.apache.spark.TaskEndReason
import org.apache.spark.executor.{InputMetrics, OutputMetrics, ShuffleReadMetrics, TaskMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockStatus, RDDInfo, StorageLevel}
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST

/**
  * Created by dirceu on 09/11/15.
  */
object UberJsonProtocol {

  def accumulableInfoFromJson(json: JsonAST.JValue) =
    JsonProtocol.accumulableInfoFromJson(json)

  def applicationEndToJson(applicationEnd: SparkListenerApplicationEnd) =
    JsonProtocol.applicationEndToJson(applicationEnd)

  def applicationEndFromJson(json: JsonAST.JValue) =
    JsonProtocol.applicationEndFromJson(json)

  def accumulableInfoToJson(accumulableInfo: AccumulableInfo) =
    JsonProtocol.accumulableInfoToJson(accumulableInfo)

  def applicationStartFromJson(json: JsonAST.JValue) =
    JsonProtocol.applicationStartFromJson(json)

  def applicationStartToJson(applicationStart: SparkListenerApplicationStart) =
    JsonProtocol.applicationStartToJson(applicationStart)

  def blockManagerAddedFromJson(json: JsonAST.JValue) =
    JsonProtocol.blockManagerAddedFromJson(json)

  def blockManagerAddedToJson(
    blockManagerAdded: SparkListenerBlockManagerAdded
  ) = JsonProtocol.blockManagerAddedToJson(blockManagerAdded)

  def blockManagerIdFromJson(json: JsonAST.JValue) =
    JsonProtocol.blockManagerIdFromJson(json)

  def blockManagerIdToJson(blockManagerId: BlockManagerId) =
    JsonProtocol.blockManagerIdToJson(blockManagerId)

  def blockManagerRemovedFromJson(json: JsonAST.JValue) =
    JsonProtocol.blockManagerRemovedFromJson(json)

  def blockManagerRemovedToJson(
    blockManagerRemoved: SparkListenerBlockManagerRemoved
  ) = JsonProtocol.blockManagerRemovedToJson(blockManagerRemoved)

  def blockStatusFromJson(json: JsonAST.JValue) =
    JsonProtocol.blockStatusFromJson(json)

  def blockStatusToJson(blockStatus: BlockStatus) =
    JsonProtocol.blockStatusToJson(blockStatus)

  def environmentUpdateFromJson(json: JsonAST.JValue) =
    JsonProtocol.environmentUpdateFromJson(json)
  def environmentUpdateToJson(
    environmentUpdate: SparkListenerEnvironmentUpdate
  ) = JsonProtocol.environmentUpdateToJson(environmentUpdate)
  def exceptionFromJson(json: JsonAST.JValue) =
    JsonProtocol.exceptionFromJson(json)
  def exceptionToJson(exception: Exception) =
    JsonProtocol.exceptionToJson(exception)

  def executorAddedFromJson(json: JsonAST.JValue) =
    JsonProtocol.executorAddedFromJson(json)

  def executorAddedToJson(executorAdded: SparkListenerExecutorAdded) =
    JsonProtocol.executorAddedToJson(executorAdded)

  def executorInfoFromJson(json: JsonAST.JValue) =
    JsonProtocol.executorInfoFromJson(json)

  def executorInfoToJson(executorInfo: ExecutorInfo) =
    JsonProtocol.executorInfoToJson(executorInfo)

  def executorMetricsUpdateFromJson(json: JsonAST.JValue) =
    JsonProtocol.executorMetricsUpdateFromJson(json)
  def executorMetricsUpdateToJson(
    metricsUpdate: SparkListenerExecutorMetricsUpdate
  ) = JsonProtocol.executorMetricsUpdateToJson(metricsUpdate)

  def executorRemovedFromJson(json: JsonAST.JValue) =
    JsonProtocol.executorRemovedFromJson(json)
  def executorRemovedToJson(executorRemoved: SparkListenerExecutorRemoved) =
    JsonProtocol.executorRemovedToJson(executorRemoved)
  //def inputMetricsFromJson(json: JsonAST.JValue) =
  //  JsonProtocol.inputMetricsFromJson(json)
  //def inputMetricsToJson(inputMetrics: InputMetrics) =
  //  JsonProtocol.inputMetricsToJson(inputMetrics)
  def jobEndFromJson(json: JsonAST.JValue) = JsonProtocol.jobEndFromJson(json)
  def jobEndToJson(jobEnd: SparkListenerJobEnd) =
    JsonProtocol.jobEndToJson(jobEnd)
  def jobResultFromJson(json: JsonAST.JValue) =
    JsonProtocol.jobResultFromJson(json)
  def jobResultToJson(jobResult: JobResult) =
    JsonProtocol.jobResultToJson(jobResult)
  def jobStartFromJson(json: JsonAST.JValue) =
    JsonProtocol.jobStartFromJson(json)
  def jobStartToJson(jobStart: SparkListenerJobStart) =
    JsonProtocol.jobStartToJson(jobStart)
  def logStartFromJson(json: JsonAST.JValue) =
    JsonProtocol.logStartFromJson(json)
  def logStartToJson(logStart: SparkListenerLogStart) =
    JsonProtocol.logStartToJson(logStart)
  def mapFromJson(json: JsonAST.JValue) = JsonProtocol.mapFromJson(json)
  def mapToJson(map: Map[String, String]) = JsonProtocol.mapToJson(map)
  //def outputMetricsFromJson(json: JsonAST.JValue) =
  //  JsonProtocol.outputMetricsFromJson(json)
  //def outputMetricsToJson(outputMetrics: OutputMetrics) =
  //  JsonProtocol.outputMetricsToJson(outputMetrics)
  def propertiesFromJson(json: JsonAST.JValue) =
    JsonProtocol.propertiesFromJson(json)
  def propertiesToJson(properties: Properties) =
    JsonProtocol.propertiesToJson(properties)
  def rddInfoFromJson(json: JsonAST.JValue) =
    JsonProtocol.rddInfoFromJson(json)
  def rddInfoToJson(rddInfo: RDDInfo) = JsonProtocol.rddInfoToJson(rddInfo)
  //def shuffleReadMetricsFromJson(json: JsonAST.JValue) =
  //  JsonProtocol.shuffleReadMetricsFromJson(json)
  //def shuffleReadMetricsToJson(shuffleReadMetrics: ShuffleReadMetrics) =
  //  JsonProtocol.shuffleReadMetricsToJson(shuffleReadMetrics)
  //def shuffleWriteMetricsFromJson(json: JsonAST.JValue) =
  //  JsonProtocol.shuffleWriteMetricsFromJson(json)
  def sparkEventFromJson(json: JsonAST.JValue) =
    JsonProtocol.sparkEventFromJson(json)
  def sparkEventToJson(event: SparkListenerEvent) =
    JsonProtocol.sparkEventToJson(event)
  def stackTraceFromJson(json: JsonAST.JValue) =
    JsonProtocol.stackTraceFromJson(json)
  def stackTraceToJson(stackTrace: Array[StackTraceElement]) =
    JsonProtocol.stackTraceToJson(stackTrace)
  def stageCompletedFromJson(json: JsonAST.JValue) =
    JsonProtocol.stageCompletedFromJson(json)
  def stageCompletedToJson(stageCompleted: SparkListenerStageCompleted) =
    JsonProtocol.stageCompletedToJson(stageCompleted)
  def stageSubmittedToJson(stageSubmitted: SparkListenerStageSubmitted) =
    JsonProtocol.stageSubmittedToJson(stageSubmitted)
  def stageSubmittedFromJson(json: JsonAST.JValue) =
    JsonProtocol.stageSubmittedFromJson(json)
  def storageLevelFromJson(json: JsonAST.JValue) =
    JsonProtocol.storageLevelFromJson(json)
  def storageLevelToJson(storageLevel: StorageLevel) =
    JsonProtocol.storageLevelToJson(storageLevel)
  def taskEndFromJson(json: JsonAST.JValue) =
    JsonProtocol.taskEndFromJson(json)
  def taskEndReasonFromJson(json: JsonAST.JValue) =
    JsonProtocol.taskEndReasonFromJson(json)
  def taskEndReasonToJson(taskEndReason: TaskEndReason) =
    JsonProtocol.taskEndReasonToJson(taskEndReason)
  def taskEndToJson(taskEnd: SparkListenerTaskEnd) =
    JsonProtocol.taskEndToJson(taskEnd)
  def taskGettingResultFromJson(json: JsonAST.JValue) =
    JsonProtocol.taskGettingResultFromJson(json)
  def taskGettingResultToJson(
    taskGettingResult: SparkListenerTaskGettingResult
  ) = JsonProtocol.taskGettingResultToJson(taskGettingResult)
  def taskInfoFromJson(json: JsonAST.JValue) =
    JsonProtocol.taskInfoFromJson(json)
  def taskInfoToJson(taskInfo: TaskInfo) =
    JsonProtocol.taskInfoToJson(taskInfo)

  def taskMetricsFromJson(json: JsonAST.JValue) =
    JsonProtocol.taskMetricsFromJson(json)
  def taskMetricsToJson(taskMetrics: TaskMetrics) =
    JsonProtocol.taskMetricsToJson(taskMetrics)
  def taskStartFromJson(json: JsonAST.JValue) =
    JsonProtocol.taskStartFromJson(json)
  def taskStartToJson(taskStart: SparkListenerTaskStart) =
    JsonProtocol.taskStartToJson(taskStart)
  def unpersistRDDFromJson(json: JsonAST.JValue) =
    JsonProtocol.unpersistRDDFromJson(json)
  def unpersistRDDToJson(unpersistRDD: SparkListenerUnpersistRDD) =
    JsonProtocol.unpersistRDDToJson(unpersistRDD)
  def UUIDFromJson(json: JsonAST.JValue) = JsonProtocol.UUIDFromJson(json)
  def UUIDToJson(id: UUID) = JsonProtocol.UUIDToJson(id)

}
