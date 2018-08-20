package com.cy.domain

/***
  *
  * @param ip
  * @param time
  * @param courseId
  * @param statusCode
  * @param referer 来源
  */

case class ClickLog(ip:String, time:String, courseId:Int, statusCode:Int, referer:String)
