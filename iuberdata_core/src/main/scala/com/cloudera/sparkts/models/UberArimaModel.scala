package com.cloudera.sparkts.models

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by dirceu on 08/06/16.
  */
class UberArimaModel(override val p: scala.Int, override val d: scala.Int, override val q: scala.Int,
                     override val coefficients: scala.Array[scala.Double],
                     override val hasIntercept: scala.Boolean = true) extends ARIMAModel(p, q, d, coefficients, hasIntercept) {
  lazy val params = Map("ArimaP" ->p.toString, "ArimaD" -> d.toString, "ArimaQ" -> q.toString)

}

object UberArimaModel {
  def fitModel(
                p: Int,
                d: Int,
                q: Int,
                ts: Vector,
                includeIntercept: Boolean = true,
                method: String = "css-cgd",
                userInitParams: Array[Double] = null): UberArimaModel = {
    val model = ARIMA.fitModel(p,d,q,ts,includeIntercept,method,userInitParams)
    new UberArimaModel(p,d,q,model.coefficients,model.hasIntercept)
  }
}
