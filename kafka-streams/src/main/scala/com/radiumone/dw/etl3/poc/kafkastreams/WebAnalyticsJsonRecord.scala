package com.radiumone.dw.etl3.poc.kafkastreams

/**
  * Created by broy on 1/17/17.
  */

case class WebAnalyticsJsonRecord (
                                tracking_id: String,
                                application_name: String,
                                application_user_id: String,
                                application_version: String,
                                application_build: String)
{

  def doMethod(): Unit = {
//    tracking_id = ""
    System.out.println("tracking id = " + tracking_id
                     + ", \napplication_name = " + application_name
                     + ", \napplication_user_id = " + application_user_id)
  }

//  override def
//   def this() {
//     this("Default value from auxiliary constructor", "", "")
//   }



}
