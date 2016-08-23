package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 01/12/15.
  */
case class JobStart(appId: String, jobId: Int, time: Long,  properties: Map[String,String] =
Map.empty[String,String]) {
}

case class JobEnd(appId: String, jobId: Int, time: Long, jobResult: String) extends Mappable

