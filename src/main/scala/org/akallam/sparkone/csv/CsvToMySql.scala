package org.akallam.sparkone.csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.akallam.sparkone.writeData.ToSQL

class CsvToMySql(sc: SparkContext, sqlContext: SQLContext) {
  def csvToMySql (csvFilter: String, selectCols: String, inputPath: String, mySqlUrl: String, 
                  mySqlTable: String, mySqlUser: String, mySqlPass: String): Unit = {
    import sqlContext.implicits._
    
    val csvDF = sqlContext.read.format("org.databricks.spark.csv").
                            option("header", "true").option("inferSchema", "true").load(inputPath);
    
    val csvTable = csvDF.registerTempTable("csvTempTable")
    val csvSelectedCols = sqlContext.sql("select "+selectCols+" from csvTable")
    
    val csvRDD = csvSelectedCols.map(x => x.toString())
    
    val filterRDD = csvRDD.filter(_.contains(csvFilter))
    
    val csvToSql = new ToSQL()
    csvToSql.toSQL(filterRDD.toDF(), mySqlUrl, mySqlTable, mySqlUser, mySqlPass)
    
    
  }
}