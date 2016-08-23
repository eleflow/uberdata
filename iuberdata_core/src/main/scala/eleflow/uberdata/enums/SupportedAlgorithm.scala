/*
* Copyright 2015 eleflow.com.br.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

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