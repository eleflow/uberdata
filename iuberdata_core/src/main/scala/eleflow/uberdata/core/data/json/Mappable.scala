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

package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 17/12/15.
  */
abstract class Mappable {
  type MapClass

  def toMap: Map[String, String] = {
    def matching(value: Any): String = {
      value match {
        case s: String => s
        case Some(s) => matching(s)
        case None => null
        case a: AnyRef => a.toString
      }
    }

    (Map[String, String]() /: this.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> {
        matching(f.get(this))
      })
    }
  }
}
