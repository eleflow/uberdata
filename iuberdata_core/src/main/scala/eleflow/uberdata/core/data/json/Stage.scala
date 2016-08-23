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

import org.apache.spark.storage.{BlockStatus, StorageLevel, RDDInfo}

/**
  * Created by dirceu on 30/11/15.
  */
case class StageJobRelation(appId: String,
                 stageId: Int,
                            attemptId:Int,
jobId:Int)

case class Stage(appId: String,
                 stageId: Int,
                 attemptId: Int,
                 name: String,
                 numTasks: Int,
                 rddsInfo: Seq[UberRDDInfo],
                 parentIds: Seq[Int],
                 details: String,
                 submissionTime: Option[Long] = None,
                 completionTime: Option[Long] = None,
                 failureReason: Option[String] = None,
                 properties: Map[String, String] = Map.empty[String, String]) //extends Mappable
{


  def buildAccumulableInfoMap(accumulables: Map[Long, AccumulableInfo]): Map[Long, Map[String, Any]] = {
    accumulables.map {
      case (id, accumulable) => id -> Map(
        "id" -> accumulable.id,
        "name" -> accumulable.name,
        "update" -> accumulable.update,
        "value" -> accumulable.value,
        "internal" -> accumulable.internal
      )
    }
  }
}

case class AccumulableInfo(appId: String, stageId: Int, id: Long, name: String, update: Option[String] = None, value: String,
                           internal: Boolean) {
  def toMap = Map(
    "id" -> id, "name" -> name, "update" -> update, "value" -> value, "internal" -> internal
  )
}

case class StorageLvl(useDisk: Boolean,
                      useMemory: Boolean,
                      useOffHeap: Boolean,
                      deserialized: Boolean,
                      replication: Int = 1) {
  def this(storage: StorageLevel) =
    this(storage.useDisk, storage.useMemory, storage.useOffHeap,
      storage.deserialized, storage.replication)

  def apply(storage: StorageLevel) = {
    new StorageLvl(storage)
  }
}

case class UberRDDInfo(id: Int,
                       name: String,
                       numPartitions: Int,
                       var storageLevel: StorageLvl,
                       parentIds: Seq[Int],
                       scope: Option[String] = None,
                       numCachedPartitions: Int = 0,
                       memSize: Long = 0L,
                       diskSize: Long = 0L,
                       externalBlockStoreSize: Long = 0L,
                       isCached: Boolean) {
  def this(rddInfo: RDDInfo) = this(rddInfo.id, rddInfo.name, rddInfo.numPartitions, StorageLvl(rddInfo.storageLevel.useDisk,
    rddInfo.storageLevel.useMemory, rddInfo.storageLevel.useOffHeap, rddInfo.storageLevel.deserialized, rddInfo.storageLevel.replication)
    , rddInfo.parentIds, rddInfo.scope.map(_.toJson), rddInfo.numCachedPartitions, rddInfo.memSize
    , rddInfo.diskSize, rddInfo.externalBlockStoreSize, rddInfo.isCached)
}

case class UberBlockStatus(storageLevel: StorageLvl,
                           memSize: Long,
                           diskSize: Long,
                           externalBlockStoreSize: Long,
                           isCached: Boolean) {
  def this(blockStatus: BlockStatus) = this(new StorageLvl(blockStatus.storageLevel),
    blockStatus.memSize,
    blockStatus.diskSize,
    blockStatus.externalBlockStoreSize,
    blockStatus.isCached)

  def apply(blockStatus: BlockStatus) = new UberBlockStatus(blockStatus)
}