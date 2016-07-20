package eleflow.uberdata.core.listener


import java.net.URI

import eleflow.uberdata.core.IUberdataContext
import eleflow.uberdata.core.conf.UberdataEventConfig
import eleflow.uberdata.core.data.json.{Stage =>StageJson}
import eleflow.uberdata.core.data.json._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.scheduler._

import org.json4s._
import play.api.libs.json.Json

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.collection.JavaConversions._


/**
  * Created by dirceu on 09/11/15.
  */
class UberdataSparkListener(sparkConf: SparkConf) extends SparkListener {

  lazy val initTimestamp = System.currentTimeMillis()

  lazy val conf = new Configuration()

  lazy val fs = FileSystem.get(new URI(pathName), conf)

  var lastStored: Long = System.currentTimeMillis()

  lazy val interval = 1 minutes


  implicit val formats = DefaultFormats

  lazy val eventsToStore: ListBuffer[String] = ListBuffer.empty[String]

  def storeEvents(force: Boolean = false) = synchronized {
    if (force || System.currentTimeMillis() > (lastStored + interval.toMillis)) {
      val outputStream = createOutputStream()

      eventsToStore.filter(!_.isEmpty).foreach { f =>
        outputStream.writeBytes(f+"\n")
      }
      eventsToStore.clear()
      outputStream.flush()
      outputStream.close()
      lastStored = System.currentTimeMillis()
    }
  }

  def pathName = UberdataEventConfig.buildPathName(sparkConf)

  def path = new Path(pathName)

  def eventsAccum[T](event: T, name:Option[String]=None)(implicit evidence$1: play.api.libs.json.Writes[T]) = synchronized {
    val eventName = name.getOrElse(event.getClass.getSimpleName)
    val stringFied = event match {
      case t:TaskEnd => s"$eventName\t${Json.stringify(Json.toJson(t.toMap))}"
      case _ => s"$eventName\t${Json.stringify(Json.toJson(event))}"
    }
    eventsToStore += stringFied
      storeEvents()
  }

  def createOutputStream(): FSDataOutputStream = if (fs.exists(path)) {
    Thread.sleep(5)
    createOutputStream()
  } else {
    fs.create(path)
  }

  def writePretty(tobeWriten: AnyRef) = s"$tobeWriten"

  def buildAccumulables(stageInfo: StageInfo) = stageInfo.accumulables.map {
    case (key, accumulableInfo) => eleflow.uberdata.core.data.json.AccumulableInfo(sparkConf.getAppId,
      stageInfo.stageId, accumulableInfo.id, accumulableInfo.name,
      accumulableInfo.update, accumulableInfo.value, accumulableInfo.internal)
  }

  import eleflow.uberdata.core.json.SparkJsonMapper._

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val accumulable = buildAccumulables(stageInfo)
    val rddInfos = stageInfo.rddInfos.map {
      rddInfo => new UberRDDInfo(rddInfo)
    }
    val stageComp = StageJson(sparkConf.getAppId, stageInfo.stageId, stageInfo.attemptId,
      stageInfo.name, stageInfo.numTasks, rddInfos, stageInfo.parentIds, stageInfo.details,
      stageInfo.submissionTime, stageInfo.completionTime, stageInfo.failureReason)
    accumulable.foreach(f => eventsAccum(f))
    eventsAccum(stageComp, Some("StageCompleted"))
    super.onStageCompleted(stageCompleted)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val stageInfo = stageSubmitted.stageInfo
    val accumulable = buildAccumulables(stageInfo)
    val rddInfos = stageInfo.rddInfos.map(new UberRDDInfo(_))
    val props = stageSubmitted.properties
    import scala.collection.JavaConversions._
    val properties = stageSubmitted.properties.stringPropertyNames().map { property =>
      property -> props.get(property).toString
    }

    val stageSub = StageJson(sparkConf.getAppId, stageInfo.stageId, stageInfo.attemptId,
      stageInfo.name, stageInfo.numTasks, rddInfos, stageInfo.parentIds, stageInfo.details,
      stageInfo.submissionTime, stageInfo.completionTime, stageInfo.failureReason,
      properties.toMap)
    accumulable.foreach(eventsAccum(_))
    eventsAccum(stageSub, Some("StageSubmitted"))
    super.onStageSubmitted(stageSubmitted)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskInfo = taskStart.taskInfo
    val task = TaskStart(sparkConf.getAppId, taskStart.stageId, taskStart.stageAttemptId, taskInfo.taskId,
      taskInfo.index, taskInfo.attemptNumber, taskInfo.launchTime, taskInfo.executorId, taskInfo.host,
      taskInfo.taskLocality.toString, taskInfo.speculative)
    eventsAccum(task)
    super.onTaskStart(taskStart)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    val taskInfo = taskGettingResult.taskInfo
    val task = TaskGettingResult(sparkConf.getAppId, taskInfo.taskId, taskInfo.index, taskInfo.attemptNumber,
      taskInfo.launchTime, taskInfo.executorId, taskInfo.host, taskInfo.taskLocality.toString, taskInfo.speculative)
    eventsAccum(task)
    super.onTaskGettingResult(taskGettingResult)
  }

  private def extractTaskMetrics(taskMetrics:Option[TaskMetrics]) ={
    val outputMetrics = taskMetrics.flatMap(_.outputMetrics)
    val shuffleWriteMetrics = taskMetrics.flatMap(_.shuffleWriteMetrics)
    val shuffleReadMetrics = taskMetrics.flatMap(_.shuffleReadMetrics)
    val inputMetrics = taskMetrics.flatMap(_.inputMetrics)
    (outputMetrics,shuffleWriteMetrics,shuffleReadMetrics,inputMetrics)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo

    val taskMetrics = Some(taskEnd.taskMetrics)
    val (outputMetrics,shuffleWriteMetrics,shuffleReadMetrics,inputMetrics) = extractTaskMetrics(taskMetrics)
    val bytesRead = inputMetrics.map(_.bytesRead)
    val recordsRead = inputMetrics.map(_.recordsRead)

    val recordsWritten = outputMetrics.map(_.recordsWritten)

    val remoteBlocksFetched = shuffleReadMetrics.map(_.remoteBlocksFetched)
    val localBlocksFetched = shuffleReadMetrics.map(_.localBlocksFetched)

    val fetchWaitTime = shuffleReadMetrics.map(_.fetchWaitTime)
    val remoteBytesRead = shuffleReadMetrics.map(_.remoteBytesRead)

    val localBytesRead = shuffleReadMetrics.map(_.localBytesRead)
    val totalBytesRead = shuffleReadMetrics.map(_.totalBytesRead)
    val totalBlocksFetched = shuffleReadMetrics.map(_.totalBlocksFetched)

    val shuffleRecordsRead = shuffleReadMetrics.map(_.recordsRead)
    val shuffleBytesWritten = shuffleWriteMetrics.map(_.shuffleBytesWritten)

    val shuffleWriteTime = shuffleWriteMetrics.map(_.shuffleWriteTime)
    val shuffleRecordsWritten = shuffleWriteMetrics.map(_.shuffleRecordsWritten)
    val reason = taskEnd.reason match {
      case Success => "Success"
      case Resubmitted => "Resubmitted"
      case TaskKilled => "TaskKilled"
      case UnknownReason => "UnknownReason"
      case TaskResultLost => "TaskResultLost"
      case _ => "Failed"
    }
    val task = new TaskEnd(sparkConf.getAppId, taskEnd.stageId, taskEnd.stageAttemptId, taskEnd.taskType,
      taskInfo.taskId, reason, taskInfo.index, taskInfo.attemptNumber, taskInfo.launchTime, taskInfo.executorId,
      taskInfo.host, taskInfo.taskLocality.toString, taskInfo.speculative, taskMetrics.map(_.hostname).getOrElse(""),
        taskMetrics.map(_.executorDeserializeTime).getOrElse(0l),taskMetrics.map(_.executorRunTime).getOrElse(0l),
        taskMetrics.map(_.resultSize).getOrElse(0l), taskMetrics.map(_.jvmGCTime).getOrElse(0l),
      taskMetrics.map(_.resultSerializationTime).getOrElse(0l),
      taskMetrics.map(_.memoryBytesSpilled).getOrElse(0l), taskMetrics.map(_.diskBytesSpilled).getOrElse(0l),
      inputMetrics.map(_.readMethod.toString), bytesRead, recordsRead, outputMetrics.map(_.writeMethod.toString),
      outputMetrics.map(_.bytesWritten), recordsWritten, remoteBlocksFetched,
      localBlocksFetched, fetchWaitTime, remoteBytesRead,
      localBytesRead, totalBytesRead, totalBlocksFetched,
      shuffleRecordsRead, shuffleBytesWritten, shuffleWriteTime,
      shuffleRecordsWritten, taskMetrics.flatMap(_.updatedBlocks.map(f => f.map {
        case (blockId, blockStatus) => UberBlockId(blockId.name, new UberBlockStatus(blockStatus))
      })).getOrElse(
        Seq.empty[UberBlockId]))
    eventsAccum(task.toMap,Some("TaskEnd"))

    taskMetrics.flatMap(_.updatedBlocks.map(f => f.map {
      case (blockId, blockStatus) => BlockMetrics(blockId.name,task.executorRunTime)
    })).getOrElse(Seq.empty[BlockMetrics]).foreach(eventsAccum(_))

        super.onTaskEnd(taskEnd)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val stagesInfo = jobStart.stageInfos.map {
      f =>
        val rddInfos = f.rddInfos.map(new UberRDDInfo(_))
        StageJson(sparkConf.getAppId, f.stageId,
          f.attemptId, f.name, f.numTasks,
          rddInfos,
          f.parentIds, f.details)
    }

    val props = jobStart.properties
    val properties = jobStart.properties.stringPropertyNames().map { property =>
      property -> props.get(property).toString
    }
    val job = JobStart(sparkConf.getAppId, jobStart.jobId, jobStart.time, properties.toMap)
    stagesInfo.foreach{
      stage => eventsAccum(StageJobRelation(stage.appId,stage.stageId,stage.attemptId,job.jobId))
    }
    eventsAccum(job)
    super.onJobStart(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val job = JobEnd(sparkConf.getAppId, jobEnd.jobId, jobEnd.time, jobEnd.jobResult.toString)
    eventsAccum(job)
    super.onJobEnd(jobEnd)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = super.onEnvironmentUpdate(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    val blockManagerId = blockManagerAdded.blockManagerId

    val block = BlockManagerAdded(blockManagerAdded.time,blockManagerId.executorId,
      blockManagerId.host,blockManagerId.port,
      blockManagerAdded.maxMem)
    eventsAccum(block)

    super.onBlockManagerAdded(blockManagerAdded)
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    val blockManagerId = blockManagerRemoved.blockManagerId
    val block = BlockManagerRemoved(blockManagerRemoved.time,blockManagerId.executorId,
      blockManagerId.host,blockManagerId.port)
    eventsAccum(block)

    super.onBlockManagerRemoved(blockManagerRemoved)
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = super.onUnpersistRDD(unpersistRDD)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val list = applicationStart.driverLogs.map(f => f.map{case (key,value)=> KeyValue(key,value)}.toList).getOrElse(List.empty[KeyValue])
    val keyv = new KeyValueList(list)
    val appStart = SparkApplicationStart(applicationStart.appName,applicationStart.appId,
      applicationStart.time,applicationStart.sparkUser,applicationStart.appAttemptId,
      keyv)
eventsAccum(appStart)
    super.onApplicationStart(applicationStart)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    storeEvents(true)
    eventsAccum(applicationEnd)
    super.onApplicationEnd(applicationEnd)
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
     executorMetricsUpdate.taskMetrics.foreach { f =>
      val (outputMetrics,shuffleWriteMetrics,shuffleReadMetrics,inputMetrics) = extractTaskMetrics(Some(f._4))
      val bytesRead = inputMetrics.map(_.bytesRead)
      val recordsRead = inputMetrics.map(_.recordsRead)

      val recordsWritten = outputMetrics.map(_.recordsWritten)

      val remoteBlocksFetched = shuffleReadMetrics.map(_.remoteBlocksFetched)
      val localBlocksFetched = shuffleReadMetrics.map(_.localBlocksFetched)

      val fetchWaitTime = shuffleReadMetrics.map(_.fetchWaitTime)
      val remoteBytesRead = shuffleReadMetrics.map(_.remoteBytesRead)

      val localBytesRead = shuffleReadMetrics.map(_.localBytesRead)
      val totalBytesRead = shuffleReadMetrics.map(_.totalBytesRead)
      val totalBlocksFetched = shuffleReadMetrics.map(_.totalBlocksFetched)

      val shuffleRecordsRead = shuffleReadMetrics.map(_.recordsRead)
      val shuffleBytesWritten = shuffleWriteMetrics.map(_.shuffleBytesWritten)

      val shuffleWriteTime = shuffleWriteMetrics.map(_.shuffleWriteTime)
      val shuffleRecordsWritten = shuffleWriteMetrics.map(_.shuffleRecordsWritten)

      val executorMetrics = ExecutorMetricsUpdated(executorMetricsUpdate.execId,f._1,f._2,f._3,
        bytesRead,recordsRead,outputMetrics.map(_.writeMethod.toString), outputMetrics.map(_.bytesWritten),
        recordsWritten,remoteBlocksFetched,localBlocksFetched,fetchWaitTime,
        remoteBytesRead,localBytesRead,totalBytesRead,totalBlocksFetched,shuffleRecordsRead,shuffleBytesWritten,
        shuffleWriteTime, shuffleRecordsWritten)
      eventsAccum(executorMetrics)
    }
    super.onExecutorMetricsUpdate(executorMetricsUpdate)
  }

  def extractExecutorMemory(conf: SparkConf) = {
    conf.getOption("spark.executor.memory").orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM")))
      .map(value =>(JavaUtils.byteStringAsBytes(value) / 1024 / 1024).toInt)
      .getOrElse(1024)
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val context = IUberdataContext.getUC.sparkContext
    val status = context.getExecutorMemoryStatus

    val executorHost = executorAdded.executorInfo.executorHost

    val executorMemory = extractExecutorMemory(context.getConf)
    val (masterHost,maxMemory,remainingMemory) = status.filter(value => new URI(s"http://${value._1}").getHost == executorHost).map{
      case (host,(maxMem,remaining)) =>
        (host,maxMem,remaining)
    }.headOption.getOrElse("",0l,0l)
    val executor = eleflow.uberdata.core.data.json.ExecutorAdded(initTimestamp,executorAdded.executorId,
      executorAdded.time, masterHost,executorHost, executorAdded.executorInfo.totalCores,
      executorAdded.executorInfo.logUrlMap, maxMemory,remainingMemory,executorMemory)
    eventsAccum(executor)
    super.onExecutorAdded(executorAdded)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {

    val executor = eleflow.uberdata.core.data.json.ExecutorRemoved(initTimestamp,executorRemoved.executorId, executorRemoved.time, executorRemoved.reason)
    eventsAccum(executor)
    super.onExecutorRemoved(executorRemoved)
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val blockUpdatedInfo = blockUpdated.blockUpdatedInfo
    val blockManagerId = blockUpdatedInfo.blockManagerId
    val block = BlockUpdated(blockManagerId.executorId,blockManagerId.host,blockManagerId.port,
      blockUpdatedInfo.blockId.name,StorageLvl(blockUpdatedInfo.storageLevel.useDisk,
        blockUpdatedInfo.storageLevel.useMemory,blockUpdatedInfo.storageLevel.useOffHeap,
        blockUpdatedInfo.storageLevel.deserialized,blockUpdatedInfo.storageLevel.replication),
      blockUpdatedInfo.memSize,blockUpdatedInfo.diskSize,blockUpdatedInfo.externalBlockStoreSize)
    eventsAccum(block)
    super.onBlockUpdated(blockUpdated)
  }
}
