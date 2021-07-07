package com.atguigu.userprofile.app

import java.util.Properties

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.dao.TagInfoDAO
import com.atguigu.userprofile.util.{ClickhouseUtil, MyPropertiesUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TaskExportCkApp {

//  1  要生成clickhouse 的宽表
//  2  读取hive的宽表  读取成为Dataframe
//  3   把Dataframe 写入到clickhouse中

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("task_export_ck_app").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //  1  要生成clickhouse 的宽表
    // 1.1   读取到标签集合的定义 ，提取标签的编码作为   宽表的标签字段
    val tagList: List[TagInfo] = TagInfoDAO.getTagInfoList()
    //1.2   宽表的定义
    // 为了保证操作的幂等性，每次执行 要把当天的宽表 清除一次
    // drop table

    //  create table  user_tag_merge_20210703  ( uid String , tagcode1 String ,tagcode2 String ,....)
    // engine= MergeTree
    // order by  uid
    val taskId: String = args(0)
    val taskDate: String = args(1)

    val tagCodeSql=tagList.map(_.tagCode.toLowerCase+" String").mkString(",")

    val tableName=s"user_tag_merge_"+taskDate.replace("-","")

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val hdfsPath: String = properties.getProperty("hdfs-store.path")
    val upDBName: String = properties.getProperty("user-profile.dbname")
    val dwDbName: String = properties.getProperty("data-warehouse.dbname")

    val dropTableSQL=s" drop table if exists  $upDBName.$tableName"

    val createTableSql=
      s"""
         |  create table   $upDBName.$tableName  ( uid String , $tagCodeSql  )
         |   engine= MergeTree
         |  order by  uid
       """.stripMargin

    println(dropTableSQL)
    println(createTableSql)
    //通过工具类执行 删表和建表语句
    ClickhouseUtil.executeSql(dropTableSQL)
    ClickhouseUtil.executeSql(createTableSql)




    //  2  读取hive的宽表  读取成为Dataframe
    val dataFrame: DataFrame = sparkSession.sql(s"select * from  $upDBName.$tableName")
    //  3   把Dataframe 写入到clickhouse中   利用jdbc
    val clickhouseUrl: String = properties.getProperty("clickhouse.url")

    dataFrame.write.mode(SaveMode.Append)
      .option("batchsize", "100")
      .option("isolationLevel", "NONE") // 关闭事务
      .option("numPartitions", "4") // 设置并发
      .option("driver","ru.yandex.clickhouse.ClickHouseDriver")
      .jdbc(clickhouseUrl,tableName,new Properties())



  }

}
