package com.praful.spark

object TestInterview2 {
  
  def main(args: Array[String]) {
    
    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    println(topicsSet)
    println(kafkaParams)
	println(kafkaParams1)
	  println(kafkaParams1)
    
  }
  
}
