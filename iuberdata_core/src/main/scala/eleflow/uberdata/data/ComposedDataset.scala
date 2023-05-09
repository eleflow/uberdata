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

import java.text.{DecimalFormatSymbols, DecimalFormat}
import java.util.Locale
import eleflow.uberdata.core.data.UberDataset
import org.apache.spark.rdd.RDD

/**
  * Created by dirceu on 18/02/15.
  */
class ComposedDataset(train: UberDataset, test: UberDataset, result: Option[RDD[(Double, Double)]]) {

  def exportResult(path: String, locale: Locale = Locale.ENGLISH) = {
    val formatSymbols = new DecimalFormatSymbols(locale)
    val formatter =
      new DecimalFormat("###############.################", formatSymbols)
    result.map(
      res =>
        res
          .coalesce(1)
          .map {
            case (id, value) =>
              s"${BigDecimal(id.toString).toString},${formatter.format(value)}"
          }
          .saveAsTextFile(path)
    ) getOrElse println("No result to export")
  }
}
