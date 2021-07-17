package com.atguigu.userprofile.ml.app

import java.util.Properties

import com.atguigu.userprofile.ml.pipeline.MyPipeline
import com.atguigu.userprofile.util.MyPropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object BusiGenderApp {

  //    预测阶段
  //    第一部分： 利用机器学习的模型 进行预测
  //      1、  按照模型的要求提供特征数据SQL
  //    2、 把hdfs中的模型加载
  //    3 、用模型对数据进行预测
  //    4、 把预测的结果进行转换 原值
  //   第二部分：
  //  1、 画像平台定义一个标签
  //  2、 程序中建标签表
  //  3、 insert into  select xxx from abc
  //  4、打包 发布到标签任务

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("busi_gender_app")// .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val taskId: String = args(0)
    val taskDate: String = args(1)
    //      1、  按照模型的要求提供特征数据SQL
    println("按照模型的要求提供特征数据SQL.....")
    val featureSQL=
      s"""
         | with  user_c1
         |  as(
         |     select  user_id , category1_id ,during_time
         |     from dwd_page_log   pl  join  dim_sku_info  si  on  page_item = si.id
         |    where pl.page_id ='good_detail' and page_item_type='sku_id'    and  pl.dt='$taskDate'
         |    and si.dt='$taskDate'
         |  ),
         |  user_label
         |  as
         |  (   select id,gender   from dim_user_info ui where ui.dt='9999-99-99' and  gender is null
         |  )
         |  select
         |   user_id,
         |   c1_rk1,
         |   c1_rk2,
         |   c1_rk3,
         |   male_dur,
         |   female_dur
         |  from
         |  (
         |  select    user_id,
         |     sum(if(rk =1 ,c1,0)) c1_rk1,
         |    sum(if(rk =2 ,c1,0)) c1_rk2,
         |     sum(if(rk =3 ,c1,0)) c1_rk3,
         |    sum(if( c1 in (3,4,6) ,dur_time,0) ) male_dur,
         |    sum(if( c1 in (8,11,15) ,dur_time,0) ) female_dur
         |   from
         |  (
         |  select  user_id , cast( category1_id  as  bigint) c1 ,ct, dur_time ,
         |   row_number()over( partition by  user_id  order by ct desc   )  rk
         |   from
         |  (
         |    select  user_id , category1_id ,count(*) ct , sum(during_time) dur_time
         |    from user_c1
         |    group by   user_id , category1_id
         |   ) user_c1_ct
         |  ) user_rk
         |  group by user_id
         |  ) user_feature    join   user_label  on user_feature.user_id = user_label.id
         |
       """.stripMargin

     sparkSession.sql("use gmall2021")
    println(featureSQL)
    val dataFrame: DataFrame = sparkSession.sql(featureSQL)
    //    2、 把hdfs中的模型加载
    println("把hdfs中的模型加载.....")
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val modelPath: String = properties.getProperty("model.path")
    val myPipeline: MyPipeline = new MyPipeline().loadModel(modelPath)
    //    3 、用模型对数据进行预测
    println("用模型对数据进行预测.....")
    val predictedDataFrame: DataFrame = myPipeline.predict(dataFrame)
    //    4、 把预测的结果进行转换 原值
    println("把预测的结果进行转换原值 .....")
    val predictedWithOriginDF: DataFrame = myPipeline.convertOrigin(predictedDataFrame)
    predictedWithOriginDF.show(100,false)

    predictedWithOriginDF.createTempView("predicted_gender")

    //   第二部分：
    //  1、 画像平台定义一个标签
     // 手动平台建立

    //  2、 程序中建标签表
    //  建表语句 ( 表名  tagcode 字段名  分区  格式   存储位置)
    //   create table  tg_busi_prediction_busigender  (uid string , tag_value  string, )
    //   comment '预测性别'
    //   partitioned by  (dt string)
    //   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
    //  location  'hdfs_path/dbName/tablename'

    val hdfsPath: String = properties.getProperty("hdfs-store.path")
    val upDBName: String = properties.getProperty("user-profile.dbname")
    val dwDbName: String = properties.getProperty("data-warehouse.dbname")

    val createTableSQL=
      s"""
         |     create table if not exists $upDBName.tg_busi_prediction_busigender  (uid string , tag_value  string )
         |     comment '预测性别'
         |      partitioned by  (dt string)
         |     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
         |      location  '$hdfsPath/$upDBName/tg_busi_prediction_busigender'
       """.stripMargin



    //  3、 insert  overwrite table  tg_busi_prediction_busigender (dt='$taskDate')   // 预测男女+手填的男女
    //  select user_id,  if( prediction_origin='F','女’,'男')  tag_value  from predicted_gender
    //  union all
    //  select  id as uid,  if( gender='F','女’,'男')   as query_value
    //  from   dim_user_info where dt='9999-99-99'  and  gender  is not null
    //

    val insertSQL=
      s"""
         |insert  overwrite table  $upDBName.tg_busi_prediction_busigender  partition (dt='$taskDate')
         |     select user_id,  if( prediction_origin='F','女','男')  tag_value  from predicted_gender
         |  union all
         |     select  id as uid,  if( gender='F','女','男')   as query_value
         |    from   dim_user_info where dt='9999-99-99'  and  gender  is not null
       """.stripMargin

    println(createTableSQL)
    sparkSession.sql(createTableSQL)
    println(insertSQL)
    sparkSession.sql(insertSQL)


    //  4、打包 发布到标签任务




  }

}
