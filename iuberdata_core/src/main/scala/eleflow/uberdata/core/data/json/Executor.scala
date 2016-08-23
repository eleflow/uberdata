package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 28/12/15.
  */
case class ExecutorAdded(initTimestamp:Long,
                         executorId: String,
                         time: Long,
                         masterHost: String,
                         executorHost: String,
                         totalCores: Int,
                         logUrlMap: Map[String, String],
                         cacheMemory: Long,
                         remainingMemory: Long,
                         executorMemory:Long)

case class ExecutorRemoved(initTimestamp:Long,executorId: String, time: Long,
                           reason: String)

case class ExecutorMetricsUpdated(executorId: String,
                                  time: Long,
                                  taskId:Long, stageId:Int,
                                  bytesRead: Option[Long] = None,
                                  recordsRead: Option[Long] = None,
                                  writeMethod: Option[String],
                                  bytesWritten: Option[Long] = None,
                                  recordsWritten: Option[Long] = None,
                                  remoteBlocksFetched: Option[Int] = None,
                                  localBlocksFetched: Option[Int] = None,
                                  fetchWaitTime: Option[Long] = None,
                                  remoteBytesRead: Option[Long] = None,
                                  localBytesRead: Option[Long] = None,
                                  totalBytesRead: Option[Long] = None,
                                  totalBlocksFetched: Option[Int] = None,
                                  shuffleRecordsRead: Option[Long] = None,
                                  shuffleBytesWritten: Option[Long] = None,
                                  shuffleWriteTime: Option[Long] = None,
                                  shuffleRecordsWritten: Option[Long] = None)

case class Workers(
                  id:String,
                  host:String,
                  port:Int,
                  webuiaddress:String,
                  cores:Int,
                  coresused:Int,
                  coresfree:Int,
                  memory:Long,
                  memoryused:Long,
                  memoryfree:Long,
                  state:String,
                  lastheartbeat:Long
                  )