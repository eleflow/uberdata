package eleflow.uberdata.core.data.json

/**
  * Created by dirceu on 17/12/15.
  */
abstract class Mappable {
  type MapClass

  def toMap: Map[String, String] ={
    def matching(value:Any):String = {
     value match {
        case s: String => s
        case Some(s) => matching(s)
        case None => null
        case a: AnyRef => a.toString()
      }
    }
    (Map[String, String]() /: this.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> {
        matching(f.get(this))
      })
    }
  }


  import reflect.runtime.{universe => ru}

Option.getClass.getFields
}
