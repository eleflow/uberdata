/*
 *  Copyright 2015 eleflow.com.br.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.cloudera.sparkts.models

import org.apache.spark.ml.linalg.Vector

/**
  * Created by dirceu on 24/08/16.
  */
class UberHoltWintersModel(override val m: Int,
                           override val alpha: Double,
                           override val beta: Double,
                           override val gamma: Double)
    extends HoltWintersModel(m, alpha, beta, gamma) {
  lazy val params = Map(
    "HoltWintersAlpha" -> alpha.toString,
    "HoltWintersBeta" -> beta.toString,
    "HoltWintersGamma" -> gamma.toString
  )
}

object UberHoltWintersModel {
  def fitModelWithBOBYQA(ts: Vector,
                         m: Int): UberHoltWintersModel = {
    val model = HoltWinters.fitModelWithBOBYQA(ts, m)
    new UberHoltWintersModel(
      m,
      model.alpha,
      model.beta,
      model.gamma
    )
  }
}
