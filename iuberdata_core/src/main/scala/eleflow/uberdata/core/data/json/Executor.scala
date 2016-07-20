package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 28/12/15.
  */
case class ExecutorAdded(val initTimestamp:Long,
                         val executorId: String,
                         val time: Long,
                         val masterHost: String,
                         val executorHost: String,
                         val totalCores: Int,
                         val logUrlMap: Map[String, String],
                         val cacheMemory: Long,
                         val remainingMemory: Long,
                         executorMemory:Long)

case class ExecutorRemoved(val initTimestamp:Long,val executorId: String, val time: Long,
                           reason: String)

case class ExecutorMetricsUpdated(val executorId: String,
                                  val time: Long,
                                  taskId:Long, stageId:Int,
                                  val bytesRead: Option[Long] = None,
                                  val recordsRead: Option[Long] = None,
                                  val writeMethod: Option[String],
                                  val bytesWritten: Option[Long] = None,
                                  val recordsWritten: Option[Long] = None,
                                  val remoteBlocksFetched: Option[Int] = None,
                                  val localBlocksFetched: Option[Int] = None,
                                  val fetchWaitTime: Option[Long] = None,
                                  val remoteBytesRead: Option[Long] = None,
                                  val localBytesRead: Option[Long] = None,
                                  val totalBytesRead: Option[Long] = None,
                                  val totalBlocksFetched: Option[Int] = None,
                                  val shuffleRecordsRead: Option[Long] = None,
                                  val shuffleBytesWritten: Option[Long] = None,
                                  val shuffleWriteTime: Option[Long] = None,
                                  val shuffleRecordsWritten: Option[Long] = None)

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