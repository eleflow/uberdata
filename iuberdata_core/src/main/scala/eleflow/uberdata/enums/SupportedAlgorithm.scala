package eleflow.uberdata.enums

/**
 * Created by dirceu on 30/10/14.
 */
object SupportedAlgorithm extends Enumeration {
  type Algorithm = Value

  val ToBeDetermined = Value(0)
  val BinarySupportVectorMachines = Value(1)
  val BinaryLogisticRegressionBFGS = Value(2)
  val NaiveBayesClassifier = Value(4)
  val LinearLeastSquares = Value(5)
  val DecisionTreeAlg = Value(6)
  val BinaryLogisticRegressionSGD = Value(7)
  val ArtificialNeuralNetwork = Value(8)
  val Arima = Value(9)
  val HoltWinters = Value(10)
  val MovingAverage8 = Value(11)
  val MovingAverage16 = Value(12)
  val MovingAverage26 = Value(13)
  val FindBestForecast = Value(14)
  val XGBoostAlgorithm = Value(15)
}