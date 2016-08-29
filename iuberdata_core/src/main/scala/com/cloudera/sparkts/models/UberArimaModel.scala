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

package com.cloudera.sparkts.models

import org.apache.spark.mllib.linalg.Vector

/**
  * Created by dirceu on 08/06/16.
  */
class UberArimaModel(override val p: scala.Int,
                     override val d: scala.Int,
                     override val q: scala.Int,
                     override val coefficients: scala.Array[scala.Double],
                     override val hasIntercept: scala.Boolean = true)
    extends ARIMAModel(p, q, d, coefficients, hasIntercept) {
  lazy val params =
    Map("ArimaP" -> p.toString, "ArimaD" -> d.toString, "ArimaQ" -> q.toString)

}

object UberArimaModel {
  def fitModel(p: Int,
               d: Int,
               q: Int,
               ts: Vector,
               includeIntercept: Boolean = true,
               method: String = "css-cgd",
               userInitParams: Array[Double] = null): UberArimaModel = {
    val model =
      ARIMA.fitModel(p, d, q, ts, includeIntercept, method, userInitParams)
    new UberArimaModel(p, d, q, model.coefficients, model.hasIntercept)
  }
}
