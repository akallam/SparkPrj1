package org.akallam.sparkone.writeData

/*import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext*/
import org.apache.spark.sql.DataFrameHolder
import org.apache.spark.sql.DataFrame

class ToSQL (){
  def toSQL(dfToWrite: DataFrame, mysqlurl: String, mysqltable: String, mysqluser: String, mysqlpass: String): Unit ={
    val MySqlurl = mysqlurl
      val MySqltable = mysqltable
      val prop = new java.util.Properties
      prop.setProperty("user",mysqluser)  
      prop.setProperty("password",mysqlpass)  
      prop.setProperty("driver","com.mysql.jdbc.Driver")  
      
      // Write the filtered and selected records to MySql table      
      dfToWrite.write.mode("append").jdbc(MySqlurl, MySqltable, prop)  
      
  }
}