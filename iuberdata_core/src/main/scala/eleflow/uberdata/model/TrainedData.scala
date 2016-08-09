package eleflow.uberdata.model

import eleflow.uberdata.enums.SupportedAlgorithm._

/**
 * Created by dirceu on 31/10/14.
 */
object TypeMixin {

 case class TrainedData[+T](model: Any, algorithm: Algorithm, iterations: Int) extends Serializable
}
