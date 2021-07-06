package com.atguigu.userprofile.app

import java.util.Properties

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.dao.TagInfoDAO
import com.atguigu.userprofile.util.MyPropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TaskMergeApp {


  //1   读取到标签集合的定义 ，提取标签的编码作为   宽表的标签字段
  // 2   每天执行产生一张宽表
  // 3  把多个标签表union成高表 ，在对高表进行行转列 ，变成宽表
  //4  写入画像库中的宽表
  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("task_merge_app")
      //.setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    val taskId: String = args(0)
    val taskDate: String = args(1)

    //1   读取到标签集合的定义 ，提取标签的编码作为   宽表的标签字段
      val tagList: List[TagInfo] = TagInfoDAO.getTagInfoList()
    //2   宽表的定义
    // 为了保证操作的幂等性，每次执行 要把当天的宽表 清除一次
    // drop table

    //  create table  user_tag_merge_20210703  ( uid string , tagcode1 string ,tagcode2 string ,....)
    //  comment '标签宽表'
    //  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
    //   location  'hdfs_path/dbName/tablename'

    val tagCodeSql=tagList.map(_.tagCode.toLowerCase+" string").mkString(",")

    val tableName=s"user_tag_merge_"+taskDate.replace("-","")

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val hdfsPath: String = properties.getProperty("hdfs-store.path")
    val upDBName: String = properties.getProperty("user-profile.dbname")
    val dwDbName: String = properties.getProperty("data-warehouse.dbname")

    val dropTableSQL=s" drop table if exists  $tableName"

    val createTableSql=
      s"""
         |create table $tableName  ( uid string , $tagCodeSql )
         |     comment '标签宽表'
         |     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
         |      location  '$hdfsPath/$upDBName/$tableName'
       """.stripMargin

    println(dropTableSQL)
    println(createTableSql)
    sparkSession.sql(s"use $upDBName")
    sparkSession.sql(dropTableSQL)
    sparkSession.sql(createTableSql)



    // 3  把多个标签表union成高表 ，在对高表进行行转列 ，变成宽表
    // 3.1 把多个标签表union成高表
    // select  uid, tag_value  from tag1 where dt='$taskDate'
    // union all
    // select  uid, tag_value  from tag2 where dt='$taskDate'
    // union all
    //.....
    val tagUnionSQL: String = tagList.map(tagInfo=>  s"select uid,  tag_value , '${tagInfo.tagCode.toLowerCase}' tag_code  from  ${tagInfo.tagCode.toLowerCase} where dt='$taskDate'"   ).mkString(" union all ")

    println(tagUnionSQL)



    //3.2  pivot 行转列
    //select * from
    //(  tagUnionSQL ) tag_union
    //pivot  (     concat_ws('',collect_list(tag_value))    as  tv   for  tag_code  in ('gender','age','amount'))
    //
     val tagCodesSQL= tagList.map("'"+_.tagCode.toLowerCase+"'").mkString(",")

     val selectSQL=
       s"""
          | select * from
          |     (  $tagUnionSQL ) tag_union
          |     pivot  (     concat_ws('',collect_list(tag_value))    as  tv   for  tag_code  in ($tagCodesSQL))
        """.stripMargin
    println(selectSQL)


    //4  写入画像库中的宽表
    // insert overwrite  table  tableName    selectSQL
    val insertSQL=s"insert overwrite table $tableName $selectSQL"

    println(insertSQL)
    sparkSession.sql(insertSQL)



  }

}
