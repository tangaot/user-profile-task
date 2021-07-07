package com.atguigu.userprofile.app

import java.util.Properties

import com.atguigu.userprofile.bean.{TagInfo, TaskInfo, TaskTagRule}
import com.atguigu.userprofile.constants.ConstCode
import com.atguigu.userprofile.dao.{TagInfoDAO, TaskInfoDAO, TaskTagRuleDAO}
import com.atguigu.userprofile.util.{MyPropertiesUtil, MySqlUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TaskSQLApp {

   //      1   获得标签的定义、标签任务的SQL 、子标签的匹配规则
   //      2   如果没有表，根据标签定义规则 建立标签表
   //      3   根据标签定义和规则 查询数据仓库
   //      4   把数据写入到对应的标签表中
  def main(args: Array[String]): Unit = {

     //      1   获得标签的定义、标签任务的SQL 、子标签的匹配规则  (mysql)
     //    1.1 如何查询mysql , jdbc , 封装一个工具类
     //    1.2 用工具类分别查询
     //    tag_info , task_info , task_tag_rule
     //    一个数  string int long
     //    一行数  hashmap  javabean(UserInfo...)    tag_info  task_info
     //    一堆数   List[UserInfo]  ...           List[task_tag_rule]

     //    如何知道要执行的是哪个任务？ 可以通过args 获得 要执行的task_id 和task_date
     val sparkConf: SparkConf = new SparkConf().setAppName("task_sql_app").setMaster("local[*]")
     val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

     val taskId: String = args(0)
     val taskDate: String = args(1)

     //根据taskId 查询 tag_info , task_info , task_tag_rule
     //要定义 javabean

      var tagInfo:TagInfo=TagInfoDAO.getTagInfoByTaskId(taskId)
      val taskInfo:TaskInfo =TaskInfoDAO.getTaskInfo(taskId)
      val taskTagRuleList:List[TaskTagRule] =TaskTagRuleDAO.getTaskTagRuleListByTaskId(taskId)


     println(tagInfo)
     println(taskInfo)
     println(taskTagRuleList)

     //      2   如果没有表，根据标签定义规则 建立标签表
     // 每个标签一个表
     //  建表语句 ( 表名  tagcode 字段名  分区  格式   存储位置)
     //   create table  tableName  (uid string , tag_value  tagValueType, )
      // comment tagname
     //   partitioned by  (dt string)
     //   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
     //  location  '/hdfs_path/dbName/tablename'

     val tableName=tagInfo.tagCode.toLowerCase
     val tagValueType= tagInfo.tagValueType match {
       case  ConstCode.TAG_VALUE_TYPE_LONG => "bigint"
       case  ConstCode.TAG_VALUE_TYPE_DECIMAL => "decimal(16,2)"
       case  ConstCode.TAG_VALUE_TYPE_STRING => "string"
       case  ConstCode.TAG_VALUE_TYPE_DATE => "string"
     }


     val properties: Properties = MyPropertiesUtil.load("config.properties")
     val hdfsPath: String = properties.getProperty("hdfs-store.path")
     val upDBName: String = properties.getProperty("user-profile.dbname")
     val dwDbName: String = properties.getProperty("data-warehouse.dbname")

    val createTableSQL=
      s"""
         |    create table if not exists $upDBName.$tableName  (uid string , tag_value  $tagValueType  )
         |     comment  '${tagInfo.tagName}'
         |       partitioned by  (dt string)
         |       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
         |       location  '$hdfsPath/$upDBName/$tableName'
       """.stripMargin

     println(createTableSQL)
     sparkSession.sql(createTableSQL)

     //      3   根据标签定义和规则 查询数据仓库   select uid ,  case  query_value when  'F' then '女' when 'xxx' then 'x' end     as tag_value from  ( $SQL)
      //     3.1 先生成一个case when 语句

     val whenThenList: List[String] = taskTagRuleList.map { taskTagRule =>
       s"when  '${taskTagRule.queryValue}'  then  '${taskTagRule.subTagValue}'"
     }
    //   $dt 要替换成taskDate

     val taskSQL= taskInfo.taskSql.replace("$dt",taskDate)

     val whenThenSQL: String = whenThenList.mkString(" ")
     //如果有四级标签的匹配规则 ,则通过规则组合成case when 语句
     // 如果没有四级标签的匹配规则 ，直接使用query_value作为标签值
     var tagValueSQL=""
     if(taskTagRuleList.size>0){
        tagValueSQL=s" case  query_value  $whenThenSQL end "
     }else{
       tagValueSQL=s" query_value "
     }

     val  selectSQL=
       s""" select uid ,
          |   $tagValueSQL as tag_value from  ( $taskSQL)  tv
        """.stripMargin

     println(selectSQL)


     //      4   把数据写入到对应的标签表中      insert  select

     sparkSession.sql(s" use  $dwDbName")
      val insertSQL=s" insert overwrite table   $upDBName.$tableName  partition (dt= '$taskDate')  $selectSQL"
     println(insertSQL)
     sparkSession.sql(insertSQL)
  }

}
