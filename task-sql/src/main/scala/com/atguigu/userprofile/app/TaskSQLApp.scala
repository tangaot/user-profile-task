package com.atguigu.userprofile.app

import com.atguigu.userprofile.bean.{TagInfo, TaskInfo, TaskTagRule}
import com.atguigu.userprofile.dao.{TagInfoDAO, TaskInfoDAO, TaskTagRuleDAO}
import com.atguigu.userprofile.util.MySqlUtil

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













  }

}
