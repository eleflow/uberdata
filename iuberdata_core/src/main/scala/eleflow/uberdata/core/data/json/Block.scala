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

