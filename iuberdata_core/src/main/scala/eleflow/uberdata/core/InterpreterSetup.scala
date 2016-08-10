package eleflow.uberdata.core

import org.apache.spark.repl.SparkIMain

/**
 * Created by dirceu on 22/05/15.
 */
class InterpreterSetup {

  def setup(intp:SparkIMain) = {
    intp.interpret("import eleflow.uberdata.core.util.ClusterSettings")
    intp.interpret("import org.apache.spark.SparkContext._")
    intp.interpret("import uc._ ")
  }
}
