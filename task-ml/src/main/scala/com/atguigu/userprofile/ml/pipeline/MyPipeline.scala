package com.atguigu.userprofile.ml.pipeline

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.DataFrame

class MyPipeline {


   var pipeline:Pipeline=null

   var pipelineModel:PipelineModel=null


   def init(): MyPipeline ={
      pipeline= new Pipeline().setStages( Array(
         createLabelIndexer,
         createFeatureAssemble,
         createFeatureInderex,
         createClassifier
      ))
      this
   }


   var labelColName:String=null

   var featureColNames:Array[String]=null

   def setLabelColName(labelColName:String): MyPipeline ={
      this.labelColName=labelColName
      this
   }

   def setFeatureColNames(featureColNames:Array[String]): MyPipeline ={
      this.featureColNames=featureColNames
      this
   }

   // 区分 离散特征还是线性特征的  数值类阈值，  小于等于该值 则为离散  大于该值为线性特征
   private var maxCategories=5

   // 最大分支数
   private var maxBins=5
   // 最大树深度
   private var maxDepth=5
   //最小分支包含数据条数
   private var minInstancesPerNode=1
   //最小分支信息增益
   private var minInfoGain=0.0



   def setMaxCategories(maxCategories:Int): MyPipeline ={
      this.maxCategories=maxCategories
      this
   }
   def setMaxBins(maxBins:Int): MyPipeline ={
      this.maxBins=maxBins
      this
   }
   def setMaxDepth(maxDepth:Int): MyPipeline ={
      this.maxDepth=maxDepth
      this
   }

   def setMinInstancesPerNode(minInstancesPerNode:Int): MyPipeline ={
      this.minInstancesPerNode=minInstancesPerNode
      this
   }

   def setMinInfoGain(minInfoGain:Double): MyPipeline ={
      this.minInfoGain=minInfoGain
      this
   }


   // 1 创建标签索引
   //  把标签值 转换为矢量值
   //  （‘男' ,’女‘）   ( 0 ,1 ,2, 3 ,4,....)     按照 出现概率大小次序  概率越大  矢量越小
   // InputCol: 参考答案列名
   // OutputCol : 转换为矢量值的列名 ，自定义
   def  createLabelIndexer(): StringIndexer ={
          //创建
           val indexer = new StringIndexer()
         //参数
           indexer.setInputCol(labelColName).setOutputCol("label_index")
          //返回
            indexer
   }

   //2  创建特征集合列
   // InputCol: 特征列名
   // OutputCol : 特征集合列名 ，自定义
   def createFeatureAssemble(): VectorAssembler ={
            val assembler = new VectorAssembler()
         assembler.setInputCols(featureColNames).setOutputCol("feature_assemble")
         assembler
   }

   //3   创建特征索引列
    //   把特征集合中的原值 ，变为 矢量值 按照 出现概率大小次序  概率越大  矢量越小
    //   要识别哪些是 连续值特征（线性特征）  哪些是离散特征 （分类特征）
    def createFeatureInderex(): VectorIndexer ={
          val indexer = new VectorIndexer()
       indexer.setInputCol("feature_assemble").setOutputCol("feature_index")
         .setMaxCategories(maxCategories)
       indexer
    }

   // 4  创建分类器

   def  createClassifier(): DecisionTreeClassifier ={
          val classifier = new DecisionTreeClassifier()
          classifier.setLabelCol("label_index")
        .setFeaturesCol("feature_index")
        .setPredictionCol("prediction_col")
        .setImpurity("gini")  // 信息熵   基尼
      classifier
   }

   // 训练

   def train(dataFrame:DataFrame ): Unit ={
       pipelineModel= pipeline.fit(dataFrame)
   }

   //预测
   def predict(dataFrame:DataFrame): DataFrame ={
      val predictedDataFrame: DataFrame = pipelineModel.transform(dataFrame)
      predictedDataFrame
   }

   //打印出 完整的决策树
   def printDecisionTree(): Unit ={
      val transformer: Transformer = pipelineModel.stages(3)
      val classificationModel: DecisionTreeClassificationModel = transformer.asInstanceOf[DecisionTreeClassificationModel]
      println(classificationModel.toDebugString)
   }


   //打印出 各个特征的权重
   def printFeatureWeights(): Unit ={
      val transformer: Transformer = pipelineModel.stages(3)
      val classificationModel: DecisionTreeClassificationModel = transformer.asInstanceOf[DecisionTreeClassificationModel]
      println(classificationModel.featureImportances)

   }



}
