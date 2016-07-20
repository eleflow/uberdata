package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 23/03/16.
  */
case class SparkApplicationStart( appName: String,
                                  appId: Option[String],
                                  time: Long,
                                  sparkUser: String,
                                  appAttemptId: Option[String],
                                  driverLogs: KeyValueList)

case class KeyValue(key:String,value:String)
case class KeyValueList(list:List[KeyValue]= List.empty)