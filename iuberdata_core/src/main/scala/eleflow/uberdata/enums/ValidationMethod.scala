package eleflow.uberdata.enums

/**
 * Created by dirceu on 31/12/14.
 */
object ValidationMethod extends Enumeration{
type ValidationMethod = Value
  val LogarithmicLoss = Value
  val AreaUnderTheROC = Value
  val MeanSquaredError = Value
}
