package com.cy.Utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  val INPUT_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String): Long = {
    INPUT_FORMAT.parse(time).getTime
  }

  def parseToMinute(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {

    println(parseToMinute("2018-08-14 17:17:00"))
  }

}
