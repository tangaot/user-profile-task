package com.atguigu.userprofile.ml.train

import com.atguigu.userprofile.ml.pipeline.MyPipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object StudGenderTrain {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("stud_gender_train_app") .setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    // 1 查询数据
    println("查询数据..")
    val sql=
      s""" select  uid,
         |   case hair when '长发' then 101
         |              when '短发' then 102
         |               when  '板寸' then 103  end  as hair ,
         |    height,
         |    case skirt when '是' then 11
         |              when '否' then 12 end as skirt,
         |    case age when '00后' then 100
         |              when '90后' then 90
         |              when '80后' then 80 end as age,
         |              gender
         |      from user_profile2077.student
       """.stripMargin
    println(sql)
    val dataFrame: DataFrame = sparkSession.sql(sql)

    // 2 切分数据 分成 训练集和测试集   8:2  7:3
    println("切分数据..")
    val    Array(trainDF,testDF) = dataFrame.randomSplit(Array(0.8,0.2))

    // 3 创建 mypipeline
    println("创建 mypipeline..")
        val myPipeline: MyPipeline = new MyPipeline().setLabelColName("gender")
          .setFeatureColNames(Array("hair","height","skirt","age"))
          .setMaxDepth(6)
          .setMinInfoGain(0.1)
          .setMaxBins(64)
          .setMinInstancesPerNode(4)
          .setMaxCategories(5).init()

    //  4  进行训练
    println("进行训练..")
    myPipeline.train(trainDF)
    myPipeline.printDecisionTree()
    myPipeline.printFeatureWeights()

    //  5  进行预测
    println("进行预测..")
    val predictedDataFrame: DataFrame = myPipeline.predict(testDF)
    //  6  打印预测结果
    predictedDataFrame.show(100,false)

    // 7  把矢量预测结果转换为原始值
    println("进行转换..")
    val convertedDataFrame: DataFrame = myPipeline.convertOrigin(predictedDataFrame)
    convertedDataFrame.show(100,false)


    // 8  打印评估报告 // 总准确率   //各个选项的 召回率 和精确率
    myPipeline.printEvaluateReport(convertedDataFrame)
  }








}
