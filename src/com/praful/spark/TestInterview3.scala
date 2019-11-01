package com.praful.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object TestInterview {
  
  case class Order(serNo: Integer, orderDate: String, custName: String, orderType: String)
  
  def main(args: Array[String]) {
    
    //val conf = new SparkConf().setAppName("TestApp").
    //val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.ERROR  )
    val sc = new SparkContext("local[*]", "TestInterview")

    val sqlContext= new SQLContext(sc)
    import sqlContext.implicits._
    var order2 = sc.textFile("/user/praful_86/move").map(x => ((x.split("|")(0)).toInt, (x.split("|")(1)).toString, (x.split("|")(2)).toString, (x.split("|")(3)).toString))
    
    var order1 = sc.textFile("/user/praful_86/move").map(_.split('|')).map(a => Order(a(0).toInt, a(1), a(2), a(3))).toDF()
    println ("successful")
    order1.registerTempTable("ordertable")
    var sqlResult = sqlContext.sql("""select custName, max(orderDate) as Latest_Date, min(orderDate) as Last_Date, cast(count(serNo) as int) as No_of_visits from ordertable where  serNo <> 4 group by custName""")
    sqlResult.show()
    var dataFrameResult = order1.filter("serNo <> 4").groupBy(col("custName")).agg( max(col("orderDate")).alias("Latest_Date"), min(col("orderDate")).alias("Last_Date"), count(col("serNo")).alias("No_of_visits")).orderBy(col("custName"))               //count can be countDistinct also

    dataFrameResult.show()
	println("end of pgm")
	println("end of pgm 3")
  }  
  
}