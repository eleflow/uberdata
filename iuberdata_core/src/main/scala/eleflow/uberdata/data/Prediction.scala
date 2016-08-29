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

package eleflow.uberdata.data

import eleflow.uberdata.model.TypeMixin.TrainedData
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import eleflow.uberdata.enums.SupportedAlgorithm

/**
  * Created by celio on 10/03/16.
  */
case class Prediction(validationPrediction: RDD[(Double, Double)],
                      model: TrainedData[scala.Serializable],
                      testDataSet: RDD[((Double, Any), LabeledPoint)],
                      validationPredictionId: RDD[(Any, Double)],
                      trainPredictionId: RDD[(Any, LabeledPoint)],
                      testPredictionId: RDD[(Any, Double)])

case class MultiplePrediction(
  multiplePredictionValidation: Map[SupportedAlgorithm.Algorithm, RDD[(Double, Double)]],
  validationDataSet: RDD[((Double, Any), LabeledPoint)],
  trainDataSet: RDD[((Double, Any), LabeledPoint)],
  multiplePredictionTest: Map[SupportedAlgorithm.Algorithm, RDD[(Any, Double)]],
  testDataSet: RDD[((Double, Any), LabeledPoint)],
  models: List[TrainedData[Serializable]]
)
