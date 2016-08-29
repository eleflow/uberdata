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

import eleflow.uberdata.model.Step

/**
  * Created by dirceu on 15/06/15.
  */
trait EvolutiveAlgorithm

case class EvolutiveMiddleStart(quantity: Int) extends EvolutiveAlgorithm {}

case class EvolutiveSingleStart(steps: List[Step]) extends EvolutiveAlgorithm {
  def this(fieldsSize: Int, columnNStep: List[Int]) = {
    //TODO review this :+
    this(
      columnNStep.zipWithIndex.foldLeft(List(Step(fieldsSize, 1)))(
        (b, a) => b :+ Step(a._1, a._2 + 2)
      )
    )
    require(
      steps.forall { case Step(a, b) => a >= b },
      s"Column list should have values of columns, greater than values " +
        s"of step size in ${steps.mkString(",")}"
    )
  }
}
