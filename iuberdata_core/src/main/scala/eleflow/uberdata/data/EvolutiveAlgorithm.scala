package eleflow.uberdata.data

import eleflow.uberdata.model.Step

/**
 * Created by dirceu on 15/06/15.
 */
trait EvolutiveAlgorithm

case class EvolutiveMiddleStart(quantity:Int) extends EvolutiveAlgorithm{

}

case class EvolutiveSingleStart(steps:List[Step]) extends EvolutiveAlgorithm{
def this(fieldsSize: Int, columnNStep: List[Int]) = {
  //TODO review this :+
  this(columnNStep.zipWithIndex.foldLeft(List(Step(fieldsSize, 1)))((b, a) => b :+ Step(a._1, a._2 + 2)))
    require(steps.forall { case Step(a, b) => a >= b}, s"Column list should have values of columns, greater than values " +
      s"of step size in ${steps.mkString(",")}")
  }
}
