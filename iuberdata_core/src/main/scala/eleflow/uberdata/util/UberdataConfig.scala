package eleflow.uberdata.util

import eleflow.uberdata.core.util.DateTimeParser


/**
 * Created by dirceu on 05/12/14.
 */
object UberdataConfig {
  val dateFormatFileName = "spark.properties"
  val propertyFolder = s"uberdata-${DateTimeParser.hashCode()}"
  val tempFolder = System.getProperty("java.io.tmpdir")
}
