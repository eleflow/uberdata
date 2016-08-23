package eleflow.uberdata.data

import java.text.{DecimalFormatSymbols, DecimalFormat}
import java.util.Locale
import eleflow.uberdata.core.data.Dataset
import org.apache.spark.rdd.RDD

/**
 * Created by dirceu on 18/02/15.
 */
class ComposedDataset(train: Dataset, test: Dataset, result: Option[RDD[(Double, Double)]]) {


  def exportResult(path: String, locale:Locale= Locale.ENGLISH) = {
    val formatSymbols= new DecimalFormatSymbols(locale)
    val formatter = new DecimalFormat("###############.################",formatSymbols)
    result.map(
      res => res.coalesce(1).map {
        case (id, value) => s"${BigDecimal(id.toString).toString},${formatter.format(value)}"
      }.saveAsTextFile(path)
    ) getOrElse println("No result to export")
  }
}
