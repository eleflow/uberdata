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


case class MultiplePrediction(multiplePredictionValidation: Map[SupportedAlgorithm.Algorithm, RDD[(Double, Double)]],
                              validationDataSet: RDD[((Double, Any), LabeledPoint)],
                              trainDataSet: RDD[((Double, Any), LabeledPoint)],
                              multiplePredictionTest: Map[SupportedAlgorithm.Algorithm, RDD[(Any, Double)]],
                              testDataSet: RDD[((Double, Any), LabeledPoint)],
                              models: List[TrainedData[Serializable]])



