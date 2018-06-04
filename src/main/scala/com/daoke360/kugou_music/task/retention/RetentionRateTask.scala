package com.daoke360.kugou_music.task.retention

import com.daoke360.kugou_music.constants.LogConstants
import com.daoke360.kugou_music.utils.{JedisUtils, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession


object RetentionRateTask {

  def validateArgs(args: Array[String]) = {
    if (args.length < 2) {
      println(
        s"""${this.getClass.getName}需要两个参数：
           |1，统计的日期：yyyy-MM-dd
           |2，统计周期：数字
         """.stripMargin)
      System.exit(0)
    } else if (!Utils.validateDate(args(0))) {
      println(
        s"""
           |${this.getClass.getName}第一个参数应该是一个日期，格式为:yyyy-MM-dd
        """.stripMargin)
      System.exit(0)
    } else if (!Utils.validateNumber(args(1))) {
      println(
        s"""
           |${this.getClass.getName}第一个参数应该是一个数字
        """.stripMargin)
      System.exit(0)
    } else {
      //2018-04-22
      //2
      val runDate = Utils.parseDateToLong(args(0), "yyyy-MM-dd")
      val cycle = args(1).toInt
      //结束日期
      val nextDay = Utils.caculateDate(runDate, cycle)

      //2018-05-09 00:00:00
      //System.currentTimeMillis()==>2018-05-09 08:43:44
      //当前日期
      val toDay = Utils.parseDateToLong(Utils.formatDate(System.currentTimeMillis(), "yyyy-MM-dd"), "yyyy-MM-dd")
      if (nextDay > toDay) {
        println(
          """
            |所属的统计周期时间未到，还不能进行统计，程序退出
          """.stripMargin)
        System.exit(0)
      }

    }
  }

  /**
    * 将Scan对象转换成Base64字符串
    *
    * @param scan
    * @return
    */
  def convertScanToBase64Scan(scan: Scan) = {
    val protoScan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(protoScan.toByteArray)
  }


  /**
    *
    * @param startTime 开始时间
    * @param endTime   结束时间
    */
  def initScan(startTime: Long, endTime: Long) = {
    val scan = new Scan()
    //扫描开始位置
    scan.setStartRow(startTime.toString.getBytes())
    //扫描结束位置
    scan.setStopRow(endTime.toString.getBytes())
    //指定需要扫描log这个列族的deviceId这一列
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes())
    scan
  }

  /**
    * 从Hbase中扫描数据
    *
    * @param startTime 开始扫描的时间
    * @param endTime   结束扫描的时间
    * @param sparkSession
    * @return
    */
  def loadDataFromHbase(startTime: Long, endTime: Long, sparkSession: SparkSession) = {

    println(s"正在加载 ${Utils.formatDate(startTime, "yyyy-MM-dd HH:mm:ss")} 到  ${Utils.formatDate(endTime, "yyyy-MM-dd HH:mm:ss")} 的数据")
    //初始化一个scan
    val scan = initScan(startTime, endTime)
    val base64Scan = convertScanToBase64Scan(scan)
    //创建一个配置文件对象
    val jobConf = new JobConf(new Configuration())
    jobConf.set(TableInputFormat.SCAN, base64Scan)
    jobConf.set(TableInputFormat.INPUT_TABLE, LogConstants.LOG_HBASE_TABLE)

    val userRDD = sparkSession.sparkContext.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(t2 => {
      val result = t2._2
      val deviceId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes()))
      (deviceId, deviceId)
    })
    userRDD
  }

  /**
    *
    * @param args yyyy-MM-dd cycle
    */
  def main(args: Array[String]): Unit = {
    //验证参数
    validateArgs(args)
    //取出参数 2018-04-22 2
    val Array(runDate, cycle) = args

    val sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()

    /** 基准日的开始时间和结束时间 */
    //2018-04-22 00:00:00
    var startTime = Utils.parseDateToLong(runDate, "yyyy-MM-dd")
    //2018-04-23 00:00:00
    var endTime = Utils.caculateDate(startTime, 1)
    //加载数据
    val todayUserRDD = loadDataFromHbase(startTime, endTime, sparkSession).distinct().cache()


    /** 统计周期之后的日期开始时间和结束时间 */
    //2018-04-24 00:00:00
    startTime = Utils.caculateDate(startTime, cycle.toInt)
    //2018-04-25 00:00:00
    endTime = Utils.caculateDate(startTime, 1)
    val nextDayUserRDD = loadDataFromHbase(startTime, endTime, sparkSession).distinct()


    //统计当天的用户数
    val todayUserCount = todayUserRDD.count()
    //统计留存周期后那一天的用户数
    val nextDayUserCount = todayUserRDD.join(nextDayUserRDD).count()
    if (todayUserCount != 0) {
      val value = Utils.getScale(nextDayUserCount.toDouble / todayUserCount, 2)
      val key = s"retention_rate_${cycle}_day"
      val field = runDate
      //将结果写入redis
      JedisUtils.hset(key, field, value.toString())
    } else {
      println(
        """
          |统计当天新增用户数为零
        """.stripMargin)
    }


    sparkSession.stop()


  }

}
