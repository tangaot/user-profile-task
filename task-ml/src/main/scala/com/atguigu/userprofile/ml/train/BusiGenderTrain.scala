package com.atguigu.userprofile.ml.train

import java.util.Properties

import com.atguigu.userprofile.ml.pipeline.MyPipeline
import com.atguigu.userprofile.util.MyPropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object BusiGenderTrain {


  def main(args: Array[String]): Unit = {
//    3、 取完整的特征+label标签 SQL
//    4、定义流水线
//    5、 获得数据，把数据投入流水线   训练
//    6、  模拟预测   评估 （评分低：特征？ 算法？  参数？）
//    7、  如果模型达到要求 ， 把模型保存起来 hdfs

    val sparkConf: SparkConf = new SparkConf().setAppName("busi_gender_train_app") .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //    3、
    println("取完整的特征+label标签 SQL")
    val selectSQL=
      s"""
         |with  user_c1
         |  as(
         |     select  user_id , category1_id ,during_time
         |     from dwd_page_log   pl  join  dim_sku_info  si  on  page_item = si.id
         |    where pl.page_id ='good_detail' and page_item_type='sku_id'    and  pl.dt='2021-05-16'
         |    and si.dt='2021-05-16'
         |  ),
         |  user_label
         |  as
         |  (   select id,gender   from dim_user_info ui where ui.dt='9999-99-99' and  gender<>''
         |  )
         |  select
         |   user_id,
         |   c1_rk1,
         |   c1_rk2,
         |   c1_rk3,
         |   male_dur,
         |   female_dur,
         |   user_label.gender
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
       """.stripMargin

    sparkSession.sql(s"use gmall2021")
    println(selectSQL)
    val dataFrame: DataFrame = sparkSession.sql(selectSQL)
    //把数据拆分
    println("取把数据拆分....")
    val  Array(trainDF,testDF) = dataFrame.randomSplit(Array(0.8,0.2))


    //    4、定义流水线
    println("定义流水线....")
    val myPipeline: MyPipeline = new MyPipeline().setLabelColName("gender")
      .setFeatureColNames(Array(   "c1_rk1","c1_rk2","c1_rk3","male_dur","female_dur" ))
      .setMaxCategories(20)
      .setMaxDepth(6)
      .setMinInfoGain(0.02)
      .setMinInstancesPerNode(3)
      .setMaxBins(32)
      .init()
    //    5、 获得数据，把数据投入流水线   训练
    println("训练....")
    myPipeline.train(trainDF)

    myPipeline.printDecisionTree()
    myPipeline.printFeatureWeights()


    //    6、  模拟预测   评估 （评分低：特征？ 算法？  参数？）
    println("模拟预测....")
    val predictedDataFrame: DataFrame = myPipeline.predict(testDF)

    predictedDataFrame.show(100,false)
    println("评估报告....")
    myPipeline.printEvaluateReport(predictedDataFrame)

   // 7 存储模型
   val properties: Properties = MyPropertiesUtil.load("config.properties")
    val modelPath: String = properties.getProperty("model.path")
    myPipeline.saveModel(modelPath)




  }
}
