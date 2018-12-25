package com.phdata.solutions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Created by Kashish Nayyar on 22/12/2018.
  */

object DdosDetector extends Serializable {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: DdosDetector <zkQuorum><group> <topic> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topic, numThreads) = args

    val conf = new SparkConf().setAppName("DdosDetector").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //Create a streaming context using sparkContext.
    val ssc = new StreamingContext(sc, Seconds(5))

    //v_CheckPointDir is the checkpointing directory for streaming context.
    val v_CheckPointDir = sc.getConf.get("spark.DdosDetector.v_CheckPointDir")

    //v_OutputDir is the output directory containing attacker's IP Addresses.
    val v_OutputDir = sc.getConf.get("spark.DdosDetector.v_OutputDir")

    //v_ThresholdCnt is the maximum allowed count of requests by any IPAddress in a given timeframe.
    val v_ThresholdCnt = sc.getConf.get("spark.DdosDetector.v_ThresholdCnt").toInt

    // v_windowLength and v_SlidingInterval defines the aggregation window length and slide respectively.
    val v_WindowLength = sc.getConf.get("spark.DdosDetector.v_WindowLength").toInt
    val v_SlidingInterval = sc.getConf.get("spark.DdosDetector.v_SlidingInterval").toInt

    ssc.checkpoint(v_CheckPointDir)

    try {
      // Create a receiver using createStream.
      val messages = KafkaUtils.createStream(ssc, zkQuorum, group ,Map(topic->numThreads.toInt))

      //Fetch IP addresses from incoming Dstream and assign each Ip-Address a value 1.
      val ip_tokens = messages.map( input => input._2). map(lines => lines.split("\"")(0).split("- -")(0).trim).map(ip => (ip,1))

      // Reduce each batch and summate the occurance of an IP address in the given window duration.
      val ip_occurances = ip_tokens.reduceByKeyAndWindow(_+_,_-_,Minutes(v_WindowLength),Seconds(v_SlidingInterval))

      // Filter potential attackers based on arbitrary threshold count.
      val potentialAttackers = ip_occurances.filter{case(ip,cnt) => cnt > v_ThresholdCnt}

      //Save the output to an output directory
      potentialAttackers.foreachRDD {rdd => rdd.saveAsTextFile(v_OutputDir) }

      //start Spark Streaming Context.
      ssc.start()
      ssc.awaitTermination()
    }
    catch {
      case e: Exception => { e.printStackTrace()}
    }
    finally { sc.stop() }
  }
}