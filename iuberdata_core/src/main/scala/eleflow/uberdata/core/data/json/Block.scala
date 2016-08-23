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
  * Created by dirceu on 23/03/16.
  */
case class BlockUpdated(blockManagerExecutorId_ : String,
                        blockManagerHost : String,
                        blockManagerPort : Int,
                        blockIdName: String,
                        storageLevel: StorageLvl,
                        memSize: Long,
                        diskSize: Long,
                        externalBlockStoreSize: Long)

case class BlockManagerAdded(time: Long,
                              blockManagerExecutorId : String,
                             blockManagerHost : String,
                             blockManagerPort : Int,
                             maxMem: Long)


case class BlockManagerRemoved(time: Long,
                             blockManagerExecutorId : String,
                             blockManagerHost : String,
                             blockManagerPort : Int)

case class BlockMetrics(blockName:String,executorRunTime:Long)

