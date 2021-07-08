package com.atguigu.userprofile.app

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.constants.ConstCode
import com.atguigu.userprofile.dao.TagInfoDAO
import com.atguigu.userprofile.util.ClickhouseUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object TaskBitmapApp {


  //    1 建表  4个表: string   decimal    long     date
  //    2 要查询标签清单列表 查mysql
  //    3  insert into  select
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("task_bitmap_app")//.setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    //    1 建表  4个表: string   decimal    long     date  (提前一次手工创建)
    // clickhouse 上的表

    //    2 要查询标签列表 查mysql
    //        2.1  把标签列表 根据标签的值类型不同 ，分成四个列表
    //        每个列表组成独立的insert select 语句  ，一个四个insert
    val tagList: List[TagInfo] = TagInfoDAO.getTagInfoList()

    val tagInfoStringList = new ListBuffer[TagInfo]
    val tagInfoDecimalList = new ListBuffer[TagInfo]
    val tagInfoLongList = new ListBuffer[TagInfo]
    val tagInfoDateList = new ListBuffer[TagInfo]

    tagList.foreach { tagInfo =>
      if (tagInfo.tagValueType == ConstCode.TAG_VALUE_TYPE_STRING) {
        tagInfoStringList.append(tagInfo)
      } else if (tagInfo.tagValueType == ConstCode.TAG_VALUE_TYPE_DECIMAL) {
        tagInfoDecimalList.append(tagInfo)
      } else if (tagInfo.tagValueType == ConstCode.TAG_VALUE_TYPE_LONG) {
        tagInfoLongList.append(tagInfo)
      } else if (tagInfo.tagValueType == ConstCode.TAG_VALUE_TYPE_DATE) {
        tagInfoDateList.append(tagInfo)
      }
    }

    val taskId: String = args(0)
    val taskDate: String = args(1)
    //    3 分别 根据不同数据类型的标签 插入到对应bitmap表 insert into  select
    insertTag(tagInfoStringList, "user_tag_value_string", taskDate)
    insertTag(tagInfoDecimalList, "user_tag_value_decimal", taskDate)
    insertTag(tagInfoLongList, "user_tag_value_long", taskDate)
    insertTag(tagInfoDateList, "user_tag_value_date", taskDate)


  }

  //  插入到bitmap表
  //insert into $tableName
  //select tv.1,tv.2 , groupBitmapState(uid) ,'$taskDate' as dt
  //from
  //(
  //select  uid , arrayJoin( [ ('gender',gender) ,('agegroup' ,agegroup ),('favor',favor)]  )  tv
  //from user_tag_merge_$taskDate
  //) user_tag
  //group by tv.1,tv.2
  def insertTag(tagList: ListBuffer[TagInfo], bitMapTableName: String, taskDate: String): Unit = {

    //幂等性处理   每次插入数据前 分区清理
    //   alter table  $bitMapTableName delete where dt='$taskDate'
    val clearTableSQL=s" alter table  $bitMapTableName delete where dt='$taskDate'"
    println(clearTableSQL)
    ClickhouseUtil.executeSql(clearTableSQL)

    if (tagList.size > 0) {
      // ('gender',gender) ,('agegroup' ,agegroup ),('favor',favor)
      val tagCodeSQL: String = tagList.map(tagInfo => s"('${tagInfo.tagCode}',${tagInfo.tagCode.toLowerCase} )").mkString(",")
      val insertIntoSQL =
        s"""
           | insert into $bitMapTableName
           | select tv.1,tv.2 , groupBitmapState( cast (uid as UInt64) ) ,'$taskDate' as dt
           | from
           | (
           | select  uid , arrayJoin( [ $tagCodeSQL]  )  tv
           | from user_tag_merge_${taskDate.replace("-", "")}
           | ) user_tag
           | where tv.2 <>''
           | group by tv.1,tv.2
           |
        """.stripMargin
      println(insertIntoSQL)
      ClickhouseUtil.executeSql(insertIntoSQL)
    }

  }

}
