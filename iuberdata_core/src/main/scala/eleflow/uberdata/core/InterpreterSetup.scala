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

package eleflow.uberdata.core
import scala.tools.nsc.interpreter.IMain

/**
  * Created by dirceu on 22/05/15.
  */
class InterpreterSetup {

  def setup(intp: IMain) = {
    intp.interpret("import eleflow.uberdata.core.util.ClusterSettings")
    intp.interpret("import org.apache.spark.SparkContext._")
    intp.interpret("import uc._ ")
  }
}
