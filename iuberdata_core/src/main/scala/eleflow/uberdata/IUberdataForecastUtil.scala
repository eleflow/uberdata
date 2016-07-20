package eleflow.uberdata

import org.apache.spark.sql.Row

/**
  * Created by dirceu on 31/05/16.
  */
object IUberdataForecastUtil {

  lazy val ALGORITHM = "algorithm"
  lazy val PARAMS = "parameters"

  def convertColumnToLong(row: Row, columnIndex: Int): Row = {
    row.get(columnIndex) match {
      case s: java.sql.Timestamp =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ s.getTime) ++ after.tail :+ s
        Row(result: _*)
      case d: Double =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ d.toLong) ++ after.tail :+ d
        Row(result: _*)
      case i: Int =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ i.toLong) ++ after.tail :+ i
        Row(result: _*)
      case s: Short =>
        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = (prior :+ s.toLong) ++ after.tail :+ s
        Row(result: _*)
      case _ => row
    }
  }

  def convertColumnToLong2(row: Row, columnIndex: Int): Row = {
    val result = row.get(columnIndex) match {
      case s: java.sql.Timestamp =>
//        val (prior, after) = row.toSeq.splitAt(columnIndex)
        val result = row.toSeq :+ s.getTime
        Row(result: _*)
      case d: Double =>
        val result = row.toSeq :+ d.toLong
        Row(result: _*)
      case i: Int =>

        val result = row.toSeq :+ i.toLong
        Row(result: _*)
      case s: Short =>
        val result = row.toSeq :+ s.toLong
        Row(result: _*)
      case _ => row
    }
    result
  }
}
