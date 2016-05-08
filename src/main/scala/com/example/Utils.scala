package com.example

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by spark on 30/10/15.
 */
object Utils {

    val master: String = "local[2]"
    val conf = new SparkConf().setAppName("Mis ejemplos").setMaster(master)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

}
