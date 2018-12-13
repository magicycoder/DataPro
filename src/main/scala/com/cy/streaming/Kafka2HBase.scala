package com.cy.streaming

import com.cy.utils.DateUtils
import com.cy.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.cy.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.ListBuffer

object Kafka2HBase {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirect")
    val ssc = new StreamingContext(conf, Seconds(5))

    if (args.length != 2) {
      System.err.println("Usage: KafkaDirect <brokerlist> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val brokerMap = Map[String, String]("metadata.broker.list"->brokers)
    val topicsSet = topics.split(",").toSet

    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, brokerMap, topicsSet)
    val logs = message.map(_._2)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      var courseId = 0

      if (url.startsWith("/class")) {
        val courseIDHTML = url.split("/")(2)
        courseId = courseIDHTML.substring(0,courseIDHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),courseId,infos(3).toInt,infos(4))

    }).filter(ClickLog => ClickLog.courseId != 0)

    //cleanData.print()

    //第三阶段：统计今天到目前为止实战课程的访问量

    cleanData.map(x =>{
      (x.time.substring(0,8) + "_" + x.courseId, 1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition{ partitionsOfRecord =>
        val list = new ListBuffer[CourseClickCount]
        partitionsOfRecord.foreach(pair =>
          list.append(CourseClickCount(pair._1, pair._2))
        )
        CourseClickCountDAO.save(list)

      }
    })


    // 分来源统计
    cleanData.map(x =>{
      val referer = x.referer.replaceAll("//","/")
      val splits = referer.split("/")
      var host = ""

      if(splits.length > 2) {
        host = splits(1)
      }
      (host,x.courseId,x.time)

    }).filter(x => x._1 != "").map(x => {
      (x._3.substring(0,10) + "_" + x._1 + "_" + x._2, 1)
    }).reduceByKey(_+_).foreachRDD(rdd => {
      rdd.foreachPartition{ partitionsOfRecord =>
        val list = new ListBuffer[CourseSearchClickCount]
        partitionsOfRecord.foreach(pair =>
          list.append(CourseSearchClickCount(pair._1, pair._2))
        )
        CourseSearchClickCountDAO.save(list)

      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
