package eleflow.uberdata.model

import org.apache.spark.mllib.regression.{GeneralizedLinearModel, RegressionModel}
import eleflow.uberdata.enums.SupportedAlgorithm._

/**
 * Created by dirceu on 31/10/14.
 */
object TypeMixin {
//  type T = union[RegressionModel]#or[NaiveBayesModel]#or[GeneralizedLinearModel]#or[ANNClassifierModel]
//
 case class TrainedData[+T](model: Any, algorithm: Algorithm, iterations: Int) extends Serializable
}
