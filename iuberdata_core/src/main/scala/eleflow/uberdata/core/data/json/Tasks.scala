package eleflow.uberdata.core.data.json


/**
  * Created by dirceu on 30/11/15.
  */
case class TaskStart(appId: String, stageId: Int, stageAttemptId: Int, val taskId: Long,
                     val index: Int,
                     val attemptNumber: Int,
                     val launchTime: Long,
                     val executorId: String,
                     val host: String,
                     val taskLocality: String,
                     val speculative: Boolean) {

}

case class TaskGettingResult(appId: String, taskId: Long,
                             index: Int,
                             attemptNumber: Int,
                             launchTime: Long,
                             executorId: String,
                             host: String,
                             taskLocality: String,
                             speculative: Boolean) {

}

case class UberBlockId(name: String, status: UberBlockStatus)

class TaskEnd(val appId: String, val stageId: Int, val stageAttemptId: Int, val taskType: String,
                   val taskId: Long,
                   val taskEndReason: String,
                   val index: Int,
                   val attemptNumber: Int,
                   val launchTime: Long,
                   val executorId: String,
                   val host: String,
                   val taskLocality: String,
                   val speculative: Boolean,
                   val hostname: String,
                   val executorDeserializeTime: Long,
                   val executorRunTime: Long,
                   val resultSize: Long,
                   val jvmGCTime: Long,
                   val resultSerializationTime: Long,
                   val memoryBytesSpilled: Long,
                   val diskBytesSpilled: Long,
                   val readMethod: Option[String]= None,
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
                   val shuffleRecordsWritten: Option[Long] = None,
                   val updatedBlocks: Seq[UberBlockId]) extends Mappable with Product with Serializable {
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
    case 13 => hostname
    case 14 => executorDeserializeTime
    case 15 => executorRunTime
    case 16 => resultSize
    case 17 => jvmGCTime
    case 18 => resultSerializationTime
    case 19 => memoryBytesSpilled
    case 20 => diskBytesSpilled
    case 21 => readMethod
    case 22 => bytesRead
    case 23 => recordsRead
    case 24 => writeMethod
    case 25 => bytesWritten
    case 26 => recordsWritten
    case 27 => remoteBlocksFetched
    case 28 => localBlocksFetched
    case 29 => fetchWaitTime
    case 30 => remoteBytesRead
    case 31 => localBytesRead
    case 32 => totalBytesRead
    case 33 => totalBlocksFetched
    case 34 => shuffleRecordsRead
    case 35 => shuffleBytesWritten
    case 36 => shuffleWriteTime
    case 37 => shuffleRecordsWritten
    case 38 => updatedBlocks
  }
  
  def apply( appId: String,  stageId: Int,  stageAttemptId: Int,  taskType: String,
   taskId: Long,
   taskEndReason: String,
   index: Int,
   attemptNumber: Int,
   launchTime: Long,
   executorId: String,
   host: String,
   taskLocality: String,
   speculative: Boolean,
   hostname: String,
   executorDeserializeTime: Long,
   executorRunTime: Long,
   resultSize: Long,
   jvmGCTime: Long,
   resultSerializationTime: Long,
   memoryBytesSpilled: Long,
   diskBytesSpilled: Long,
   readMethod: Option[String]= None,
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
   shuffleRecordsWritten: Option[Long] = None,
   updatedBlocks: Seq[UberBlockId]) = new TaskEnd( appId,  stageId,  stageAttemptId,  taskType,
   taskId,
   taskEndReason,
   index,
   attemptNumber,
   launchTime,
   executorId,
   host,
   taskLocality,
   speculative,
   hostname,
   executorDeserializeTime,
   executorRunTime,
   resultSize,
   jvmGCTime,
   resultSerializationTime,
   memoryBytesSpilled,
   diskBytesSpilled,
   readMethod,
   bytesRead,
   recordsRead,
   writeMethod,
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
   updatedBlocks)

}



case class TaskEndComp(val memoryBytesSpilled: Long,
                        val diskBytesSpilled: Long,
                        val readMethod: Option[String]= None,
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
                        val shuffleRecordsWritten: Option[Long] = None,
                        val updatedBlocks: Seq[UberBlockId])
