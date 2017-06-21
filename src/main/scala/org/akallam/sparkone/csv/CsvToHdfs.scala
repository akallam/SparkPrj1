package org.akallam.sparkone.csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class CsvToHdfs(sc:SparkContext, sqlContext: SQLContext)  {
    def CSV2HDFS(csvFilter: String, selectCols: String, inputPath: String, outputPath: String): Unit = {
      // load csv to data frame
      val csvDF = sqlContext.read.format("com.databricks.spark.csv")
                      .option("header", "true").option("inferScheme", "true")
                      .load(inputPath);
      // register temp table
      csvDF.registerTempTable("CSVTable");
      
      // query current table and save it in new data set
      val csvSelectedCols = sqlContext.sql("select "+selectCols+" FROM CSVTable");
      
      // convert data frame to RDD
      val csvRDD = csvSelectedCols.map(x => x.toString());
      
      // filter this rdd
      val csvFilteredRDD = csvRDD.filter(_.contains(csvFilter));
  
      //write theserecords on HDFS
      csvFilteredRDD.saveAsTextFile(outputPath);
      
      
    }
}