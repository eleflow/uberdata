package org.apache.spark

import org.apache.spark.rdd.RDDOperationScope
import org.apache.spark.storage.StorageLevel

/**
  * Created by dirceu on 07/12/15.
  */
object UberRDDOperationScope {

  implicit def fromStrToStorage(str:String):StorageLevel = StorageLevel.fromString(str)

  implicit def fromStrToRDDOperationalScope(str: String) = RDDOperationScope.fromJson(str)

}
