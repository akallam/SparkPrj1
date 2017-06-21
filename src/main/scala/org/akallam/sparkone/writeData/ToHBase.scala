package org.akallam.sparkone.writeData

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put


class ToHBase(sc:SparkContext) {
   def toHbase(rddToWrite: RDD[String], colsArray: Array[String], hbTable: String, colFaml: String): Unit ={
     //* Write the filtered and selected records to HBase *// 
      val HbasetableName = hbTable
      
      for (j <- 0 until colsArray.length) {
        val SqlRDD = rddToWrite.map(x => x.toString().replace("[","").replace("]","").split(",")(j))
        val colName = colsArray(j)
        val SqlRDDzip = SqlRDD.zipWithIndex()
        val zkQuorum = sc.getConf.get("spark.HBASE.zkQuorum")
        val zkPort = sc.getConf.get("spark.HBASE.zkPort")
        val rootdir = sc.getConf.get("spark.HBASE.rootdir") 
        SqlRDDzip.foreach { sqlStr => 
          // HBase Configuration
          val Hconf = HBaseConfiguration.create()
          Hconf.set("hbase.zookeeper.quorum", zkQuorum)
          Hconf.set("hbase.zookeeper.property.clientPort", zkPort)
          Hconf.set("hbase.rootdir", rootdir)
          Hconf.set(TableInputFormat.INPUT_TABLE, HbasetableName)      
          
          val table = new HTable(Hconf, HbasetableName)
          
          val p = new Put(new String("SqlRowid" + sqlStr._2).getBytes())
          p.add(colFaml.getBytes(), colName.getBytes(), sqlStr._1.getBytes())          
          table.put(p)
        }
      }
   }
}