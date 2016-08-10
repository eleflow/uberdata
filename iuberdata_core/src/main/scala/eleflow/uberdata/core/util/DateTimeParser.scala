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
* @src https://openntf.org/XSnippets.nsf/snippet.xsp?id=dateutils
*/
package eleflow.uberdata.core.util

import java.nio.charset.StandardCharsets
import java.nio.file.{FileSystems, Files}
import java.text.ParseException

import eleflow.uberdata.core.conf.SparkNotebookConfig
import eleflow.uberdata.core.enums.PeriodOfDay
import org.apache.spark.SparkFiles
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConversions._
import scala.util.{Success, Try}

/**
  * Created by dirceu on 24/02/15.
  */
object DateTimeParser extends Serializable {

  val offset = DateTimeZone.getDefault.getOffset(new DateTime())

  def apply(offset: Int) = {
    new DateTimeParser(offset)
  }

  def apply() = {
    new DateTimeParser(offset)
  }
}

/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
final class DateTimeParser(offset: Int) extends Serializable {
  def parse(dateString: String): Option[DateTime] = {
    val dateFormat: Option[String] = readDateFormat.orElse(determineDateFormat(dateString))
    dateFormat.flatMap { f =>
      Try {
        parse(dateString, dateFormat)
      } match {
        case Success(s) => s
        case _ => None
      }
    }
  }

  def parse(dateString: String, dateFormat: String): Option[DateTime] = {
    val formatter = DateTimeFormat.forPattern(dateFormat).withZoneUTC()
    Some(formatter.parseDateTime(dateString).minusMillis(offset))
  }

  def parse(dateString: String, dateFormatOption: Option[String]): Option[DateTime] = {
    dateFormatOption match {
      case Some(dateFormat) =>
        parse(dateString, dateFormat)
      case None =>
        parse(dateString)
    }

  }

  def isValidDate(dateString: String): Boolean = parse(dateString).isDefined

  def isValidDate(dateString: String, dateFormat: String): Boolean = {
    try {
      parse(dateString, dateFormat)
      true
    }
    catch {
      case e: ParseException =>
        false
    }
  }

  def determineDateFormat(dateString: String): Option[String] = DATE_FORMAT_REGEXPS.keySet.find(
    regexp => dateString.toLowerCase.matches(regexp)).flatMap(f => DATE_FORMAT_REGEXPS.get(f))

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
    """^\d{1,2}\s[a-z]{4,}\s\d{4}\s\d{1,2}:\d{2}:\d{2}$""" -> "dd MMMM yyyy HH:mm:ss")

  def period(date: DateTime): PeriodOfDay.PeriodOfDay = {
    date.getHourOfDay match {
      case hour if hour < 6 => PeriodOfDay.Dawn
      case hour if hour < 12 => PeriodOfDay.Morning
      case hour if hour < 18 => PeriodOfDay.Afternoon
      case _ => PeriodOfDay.Evening
    }
  }

  lazy val dateFormatFilePath = FileSystems.getDefault.getPath(SparkNotebookConfig.tempFolder, SparkNotebookConfig.propertyFolder,
    SparkNotebookConfig.dateFormatFileName)

  private lazy val propertyFolderPath = FileSystems.getDefault.getPath(SparkNotebookConfig.tempFolder, SparkNotebookConfig.propertyFolder)

  def applyDateFormat(dateFormat: String) = {
    if (Files.notExists(propertyFolderPath)) {
      Files.createDirectory(propertyFolderPath)
    }
    Files.deleteIfExists(dateFormatFilePath)
    Files.write(dateFormatFilePath, dateFormat.getBytes)
  }

  private def readDateFormat = {
    val clusterFilePath = FileSystems.getDefault.getPath(SparkFiles.get(SparkNotebookConfig.dateFormatFileName))
    if (Files.exists(clusterFilePath)) Files.readAllLines(clusterFilePath, StandardCharsets.UTF_8).headOption
    else None
  }
}