package com.daoke360.kugou_music.task.analysislog

import java.io.IOException

import com.daoke360.kugou_music.caseclass.IPRule
import com.daoke360.kugou_music.common.LogAnalysis
import com.daoke360.kugou_music.constants.{GlobalConstants, LogConstants}
import com.daoke360.kugou_music.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object AnalysisLogTask {


  def processArgs(sparkConf: SparkConf, args: Array[String]) = {
    if (args.length >= 1 && Utils.validateDate(args(0))) {
      sparkConf.set(GlobalConstants.TASK_RUN_DATE, args(0))
    } else {
      println(
        """
          |参数有误:
          |com.daoke360.kugou_music.task.analysislog.AnalysisLogTask 需要传入一个 "yyyy-MM-dd" 格式的时间参数
          |这个参数用于程序解析这个时间的日志
        """.stripMargin)
      System.exit(0)
    }
  }

  /**
    * 验证输入路径是否存在
    *
    * @param sparkConf
    */
  def processInputPath(sparkConf: SparkConf) = {
    val runDate = sparkConf.get(GlobalConstants.TASK_RUN_DATE)
    val datePath = Utils.formatDate(Utils.parseDateToLong(runDate, "yyyy-MM-dd"), "yyyy/MM/dd")
    val inputPath = new Path(GlobalConstants.HDFS_LOG_PATH_PREFIX.concat(datePath))
    var fs: FileSystem = null
    try {
      fs = FileSystem.newInstance(new Configuration)
      if (fs.exists(inputPath)) {
        sparkConf.set(GlobalConstants.TASK_INPUT_PATH, inputPath.toString)
      } else {
        println(s"日志路径不存在:${inputPath.toString}，程序退出")
        System.exit(0)
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (fs != null) fs.close()
    }
  }

  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    //验证参数是否正确
    processArgs(sparkConf, args)
    //验证输入路径是否存在
    processInputPath(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    //加载ip规则库
    val ipRules: Array[IPRule] = sparkSession.read.textFile("/spark_sf_project/resource/ip.data").map(line => {
      val fields = line.split("[|]")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()
    //广播ip规则
    val ipRulesBroadCast = sparkSession.sparkContext.broadcast(ipRules)

    //定义两个累加器，一个是输入的条目数，一个是过滤的条目数
    val inputAccumulator = sparkSession.sparkContext.longAccumulator("inputAccumulator")
    val filterAccumulator = sparkSession.sparkContext.longAccumulator("filterAccumulator")

    //加载日志
    val logMapRDD = sparkSession.sparkContext.textFile(sparkConf.get(GlobalConstants.TASK_INPUT_PATH)).map(logText => {
      inputAccumulator.add(1)
      LogAnalysis.analysisLog(logText, ipRulesBroadCast.value)
    }).filter(logMap => {
      if (logMap == null) {
        filterAccumulator.add(1)
        false
      } else if (!logMap.contains(LogConstants.LOG_COLUMNS_NAME_DEVICE_ID)) {
        filterAccumulator.add(1)
        false
      } else {
        true
      }
    })
    val tuple2RDD = logMapRDD.map(map => {
      val accessTime = map(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME)
      val deviceId = map(LogConstants.LOG_COLUMNS_NAME_DEVICE_ID)
      val rowKey = accessTime + "_" + deviceId.hashCode
      val put = new Put(rowKey.getBytes)
      map.foreach(t2 => {
        put.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes, t2._1.getBytes, t2._2.getBytes())
      })
      (new ImmutableBytesWritable(), put)
    })
    val jobConf = new JobConf(new Configuration())
    //指定使用TableOutputFormat这个类将数据写入到hbase表中
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //输入结果目标表  create 'kugou_music_log','log'
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, LogConstants.LOG_HBASE_TABLE)
    tuple2RDD.saveAsHadoopDataset(jobConf)
    println(s"本次输入记录数：${inputAccumulator.value},过滤记录数:${filterAccumulator.value}")
    sparkSession.stop()
  }
}
