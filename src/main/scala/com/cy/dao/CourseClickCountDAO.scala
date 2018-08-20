package com.cy.dao

import com.cy.domain.CourseClickCount
import com.cy.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CourseClickCountDAO {

  val tableName = "course_click"
  val cf = "info"
  val qulifer = "click_count"

  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for(ele <- list) {
      table.incrementColumnValue(ele.day_course.getBytes,
        cf.getBytes,
        qulifer.getBytes,
        ele.click_count)
    }

  }

  def query(day_course:String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(day_course.getBytes)
    val value = table.get(get).getValue(cf.getBytes, qulifer.getBytes)

    if(value == null){
      0L
    }else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20181111_1",10))
    list.append(CourseClickCount("20181111_2",100))
    list.append(CourseClickCount("20181111_3",30))

    save(list)
    println(query("20181111_1").toString + ':' + query("20181111_2").toString + ':' + query("20181111_3").toString)
  }
}
