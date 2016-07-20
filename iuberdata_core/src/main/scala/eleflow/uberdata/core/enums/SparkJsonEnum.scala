package eleflow.uberdata.core.enums

/**
  * Created by dirceu on 27/11/15.
  */
object SparkJsonEnum extends Enumeration with Serializable {


  type JSon = Value
  val AccumulableInfo, StageCompleted, StageSubmitted, TaskStart, TaskGettingResult, TaskEnd, JobStart, JobEnd = Value
}
