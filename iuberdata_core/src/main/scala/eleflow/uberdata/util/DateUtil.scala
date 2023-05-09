package eleflow.uberdata.util

/**
  * Created by dirceu on 17/10/14.
  */
/*
 * net/balusc/util/DateUtil.java
 *
 * Copyright (C) 2007 BalusC
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if
 * not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

import java.nio.charset.StandardCharsets
import eleflow.uberdata.core.conf.SparkNotebookConfig
import eleflow.uberdata.core.enums.PeriodOfDay
import org.apache.spark.SparkFiles

import scala.jdk.CollectionConverters._
import java.nio.file.{FileSystems, Files}
import java.text.ParseException

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat

import scala.util.{Success, Try}

/**
  * Useful Date utilities.
  *
  * @author BalusC
  * @see http://balusc.blogspot.com/2007/09/dateutil.html
  * @see CalendarUtil
  */
object DateUtil extends Serializable {

  /**
    * Parse the given date string to date object and return a date instance based on the given
    * date string. This makes use of the {@link DateUtil#determineDateFormat(String)} to determine
    * the SimpleDateFormat pattern to be used for parsing.
    *
    * @param dateString The date string to be parsed to date object.
    * @return The parsed date object.
    * @throws ParseException If the date format pattern of the given date string is unknown, or if
    *                        the given date string or its actual date is invalid based on the date format pattern.
    */
  def parse(dateString: String): Option[DateTime] = {
    val dateFormat: Option[String] =
      readDateFormat.orElse(determineDateFormat(dateString))
    dateFormat.flatMap { f =>
      Try {
        parse(dateString, dateFormat)
      } match {
        case Success(s) => s
        case _ => None
      }
    }
  }

  def parse(dateString: String, timeZone: DateTimeZone): Option[DateTime] = {
    val dateFormat: Option[String] =
      readDateFormat.orElse(determineDateFormat(dateString))
    dateFormat.flatMap { f =>
      Try {
        parse(dateString, dateFormat, timeZone)
      } match {
        case Success(s) => s
        case _ => None
      }
    }
  }

  /**
    * Validate the actual date of the given date string based on the given date format pattern and
    * return a date instance based on the given date string.
    *
    * @param dateString The date string.
    * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
    * @return The parsed date object.
    * @throws ParseException If the given date string or its actual date is invalid based on the
    *                        given date format pattern.
    * @see SimpleDateFormat
    */
  def parse(dateString: String, dateFormat: String): Option[DateTime] =
    parse(dateString, dateFormat, DateTimeZone.UTC)

  def parse(dateString: String, dateFormat: String, timeZone: DateTimeZone): Option[DateTime] = {
    val formatter = DateTimeFormat.forPattern(dateFormat).withZone(timeZone)
    Some(formatter.parseDateTime(dateString))
  }

  /**
    * Either call  parse with or without dateFormat depending on the dateFormatOption
    *
    * @param dateString The date string.
    * @param dateFormatOption An Option with the date format pattern which should respect the SimpleDateFormat rules.
    * @return The parsed date object.
    * @throws ParseException If the given date string or its actual date is invalid based on the
    *                        given date format pattern.
    * @see parse(dateString: String, dateFormat: String) and parse(dateString: String)
    */
  def parse(dateString: String,
            dateFormatOption: Option[String],
            timeZone: DateTimeZone = DateTimeZone.UTC): Option[DateTime] = {
    dateFormatOption match {
      case Some(dateFormat) =>
        parse(dateString, dateFormat, timeZone)
      case None =>
        parse(dateString)
    }

  }

  /**
    * Checks whether the actual date of the given date string is valid. This makes use of the
    * {@link DateUtil#determineDateFormat(String)} to determine the SimpleDateFormat pattern to be
    * used for parsing.
    *
    * @param dateString The date string.
    * @return True if the actual date of the given date string is valid.
    */
  def isValidDate(dateString: String): Boolean = parse(dateString).isDefined

  /**
    * Checks whether the actual date of the given date string is valid based on the given date
    * format pattern.
    *
    * @param dateString The date string.
    * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
    * @return True if the actual date of the given date string is valid based on the given date
    *         format pattern.
    * @see SimpleDateFormat
    */
  def isValidDate(dateString: String, dateFormat: String): Boolean = {
    try {
      parse(dateString, dateFormat)
      true
    } catch {
      case e: ParseException => false
    }
  }

  /**
    * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
    * format is unknown. You can simply extend DateUtil with more formats if needed.
    *
    * @param dateString The date string to determine the SimpleDateFormat pattern for.
    * @return The matching SimpleDateFormat pattern, or null if format is unknown.
    * @see SimpleDateFormat
    */
  def determineDateFormat(dateString: String): Option[String] = {
    for (regexp <- DATE_FORMAT_REGEXPS.keySet) {
      if (dateString.toLowerCase.matches(regexp)) {
        Some(DATE_FORMAT_REGEXPS(regexp))
      }
    }
    None
  }

  private val DATE_FORMAT_REGEXPS: Map[String, String] = Map(
    "^\\d{8}$" -> "yyyyMMdd",
    """^\d{1,2}-\d{1,2}-\d{4}$""" -> "dd-MM-yyyy",
    """^\d{4}-\d{1,2}-\d{1,2}$""" -> "yyyy-MM-dd",
    """^\d{1,2}/\d{1,2}/\d{4}$""" -> "MM/dd/yyyy",
    """^\d{4}/\d{1,2}/\d{1,2}$""" -> "yyyy/MM/dd",
    """^\d{1,2}\s[a-z]{3}\s\d{4}$""" -> "dd MMM yyyy",
    """^\d{1,2}\s[a-z]{4,}\s\d{4}$""" -> "dd MMMM yyyy",
    """^\d{12}$""" -> """yyyyMMddHHmm""",
    """^\d{8}\s\d{4}$""" -> """yyyyMMdd HHmm""",
    """^\d{1,2}-\d{1,2}-\d{4}\s\d{1,2}:\d{2}$""" -> "dd-MM-yyyy HH:mm",
    """^\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{2}$""" -> "yyyy-MM-dd HH:mm",
    """^\d{1,2}/\d{1,2}/\\d{4}\s\d{1,2}:\d{2}$""" -> "MM/dd/yyyy HH:mm",
    """^\d{4}/\d{1,2}/\\d{1,2}\s\d{1,2}:\d{2}$""" -> "yyyy/MM/dd HH:mm",
    """^\d{1,2}\s[a-z]{3}\s\d{4}\s\d{1,2}:\d{2}$""" -> "dd MMM yyyy HH:mm",
    """^\d{1,2}\s[a-z]{4,}\s\d{4}\s\d{1,2}:\d{2}$""" -> "dd MMMM yyyy HH:mm",
    """^\d{14}$""" -> """yyyyMMddHHmmss""",
    """^\d{8}\\s\d{6}$""" -> """yyyyMMdd HHmmss""",
    """^\d{1,2}-\d{1,2}-\d{4}\s\d{1,2}:\d{2}:\d{2}$""" -> "dd-MM-yyyy HH:mm:ss",
    """^\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{2}:\d{2}$""" -> "yyyy-MM-dd HH:mm:ss",
    """^\d{1,2}/\d{1,2}/\d{4}\s\d{1,2}:\d{2}:\d{2}$""" -> "MM/dd/yyyy HH:mm:ss",
    """^\d{4}/\d{1,2}/\d{1,2}\s\d{1,2}:\d{2}:\d{2}$""" -> "yyyy/MM/dd HH:mm:ss",
    """^\d{1,2}\s[a-z]{3}\s\d{4}\s\d{1,2}:\d{2}:\d{2}$""" -> "dd MMM yyyy HH:mm:ss",
    """^\d{1,2}\s[a-z]{4,}\s\d{4}\s\d{1,2}:\d{2}:\d{2}$""" -> "dd MMMM yyyy HH:mm:ss"
  )

  def period(date: DateTime): PeriodOfDay.PeriodOfDay = {
    date.getHourOfDay match {
      case hour if hour < 6 => PeriodOfDay.Dawn
      case hour if hour < 12 => PeriodOfDay.Morning
      case hour if hour < 18 => PeriodOfDay.Afternoon
      case _ => PeriodOfDay.Evening
    }
  }

  lazy val dateFormatFilePath = FileSystems.getDefault.getPath(
    SparkNotebookConfig.tempFolder,
    SparkNotebookConfig.propertyFolder,
    SparkNotebookConfig.dateFormatFileName
  )

  private lazy val propertyFolderPath = FileSystems.getDefault.getPath(
    SparkNotebookConfig.tempFolder,
    SparkNotebookConfig.propertyFolder
  )

  def applyDateFormat(dateFormat: String) = {
    if (Files.notExists(propertyFolderPath)) {
      Files.createDirectory(propertyFolderPath)
    }
    Files.deleteIfExists(dateFormatFilePath)
    Files.write(dateFormatFilePath, dateFormat.getBytes)
  }

  private def readDateFormat = {
    val clusterFilePath = FileSystems.getDefault.getPath(
      SparkFiles.get(SparkNotebookConfig.dateFormatFileName)
    )
    if (Files.exists(clusterFilePath))
      Files.readAllLines(clusterFilePath, StandardCharsets.UTF_8).asScala.headOption
    else None
  }

}

final class DateUtil {}
