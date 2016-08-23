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

package org.apache.spark.ml.param.shared

/**
  * Created by dirceu on 25/04/16.
  */

import org.apache.spark.ml.param._

/**
  * Created by dirceu on 14/04/16.
  */
trait SplitValue extends Params {

  /**
    * Param for label column name.
    *
    * @group param
    */
  final val splitValue: Param[String] = new Param[String](this, "splitValue", "Value to be used to split train and " +
    "validation dataset")

  /** @group getParam */
  final def getSplitValue: String = $(splitValue)
}
