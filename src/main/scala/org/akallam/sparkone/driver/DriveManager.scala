package org.akallam.sparkone.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DriveManager {
  var CSV_FORMAT = "CSV"
  var XML_FORMAT = "XML"
  var JSON_FORMAT = "JSON"
  var TEXT_FORMAT = "TEXT"
  var SEQ_FORMAT = "SEQ"
  var MYSQL_INPUT = "MYSQL"
  var HDFS_TARGET = "HDFS"
  var HBASE_TARGET = "HBase"
  var KAFKA_TARGET = "Kafka"
  var MYSQL_TARGET = "MySql"
  def main(args: Array[String]): Unit = {
    if(args.length<2){
      System.err.println("Usage: DataIngestion <input fileformat> <target source>")
      System.exit(1)
    }
    val inputFormat = args(0);
    val targetSource = args(1);
    
    // configure sc and sqlContext
    val conf = new SparkConf().setAppName("sparkOne Driver").setMaster("local[*]");
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    
    if(inputFormat.equalsIgnoreCase(CSV_FORMAT)){
      
    }
    if(inputFormat.equalsIgnoreCase(XML_FORMAT)){
      
    }
    if(inputFormat.equalsIgnoreCase(JSON_FORMAT)){
      
    }
    if(inputFormat.equalsIgnoreCase(MYSQL_INPUT)){
      
    }
    
  }
}