package com.radiumone.dw.etl3.poc.kafkastreams

/**
  * Created by broy on 1/17/17.
  */
object WebAnalyticsRestPrepStream {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println("Usage: "
        + this.getClass.getName
        + "sourceTopic destintationTopic applicationIdConfig")
      System.exit(1)
    }

    val Array(sourceTopic, destinationTopic, applicationConfig) = args

  }

}
