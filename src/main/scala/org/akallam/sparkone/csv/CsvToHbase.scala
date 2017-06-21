package org.akallam.sparkone.csv

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.akallam.sparkone.writeData.ToHBase

class CsvToHbase(sc:SparkContext, sqlContext: SQLContext) {
  def csvToHbase(csvFilter:String, selectCols: String, inputPath: String, 
                  zkQuorum: String, zkPort: String, rootDir: String, hbTable: String,
                  htable: String, colFamily: String): Unit = {
    val csvDF = sqlContext.read.format("org.databricks.spark.csv").
      option("header","true").option("inferSchema", "true").load(inputPath);
    
    csvDF.registerTempTable("csvTable");
    
    val csvSelDF = sqlContext.sql("select " +selectCols+ "from csvTable");
    
    val csvSelRDD = csvSelDF.map(x => x.toString());
    
    val filterRDD = csvSelRDD.filter(_.contains(csvFilter));
    
    println(filterRDD.count());
    
    val colsArray = selectCols.split(",")
      
     val writeCSV2HBase = new ToHBase(sc);
    writeCSV2HBase.toHbase(filterRDD, colsArray, hbTable, colFamily);
      
      println("Data successfully written to HBase Table: " )
     
    
    
  }
}