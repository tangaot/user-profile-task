package com.atguigu.userprofile.dao

import com.atguigu.userprofile.bean.TagInfo
import com.atguigu.userprofile.util.MySqlUtil

object TagInfoDAO {


  def getTagInfoByTaskId(taskId:String): TagInfo ={
    var tagInfo:TagInfo=null
    val maybeTagInfo: Option[TagInfo] = MySqlUtil.queryOne(s"""select  id,tag_code,tag_name,
                                                              | parent_tag_id,tag_type,tag_value_type,
                                                              | tag_value_limit,tag_task_id,tag_comment,
                                                              | create_time
                                                              | from tag_info where  tag_task_id='$taskId'""".stripMargin,classOf[TagInfo] ,true  )
    if(maybeTagInfo!=None){
      tagInfo=maybeTagInfo.get
    }
    tagInfo
  }
}
