package com.daoke360.kugou_music.task.tags

import java.util.Properties

import com.daoke360.kugou_music.bean.Logs
import com.daoke360.kugou_music.caseclass.{AppBehavior, UserInterested}
import com.daoke360.kugou_music.constants.{LogConstants, TagsConstants}
import com.daoke360.kugou_music.tags.{Tags4AppBehavior, Tags4Device, Tags4UserInterested}
import com.daoke360.kugou_music.utils.Utils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


object MartTagForUserTask {


  def validateArgs(args: Array[String]) = {
    if (args.length < 1 && !Utils.validateDate(args(0))) {
      println(s"${this.getClass.getName}需要一个 :yyyy-MM-dd 格式的日期参数")
      System.exit(0)
    }
  }

  /**
    * 初始化扫描器
    *
    * @param startTime
    * @param endTime
    * @return
    */
  def initScan(startTime: Long, endTime: Long) = {
    val scan = new Scan()
    scan.setStartRow(startTime.toString.getBytes())
    scan.setStopRow(endTime.toString.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_ALBUM_ID.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_ANCHOR_ID.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_PLAY_TIME.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_OS_NAME.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_OS_VERSION.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_MODEL_NUM.getBytes())
    scan
  }


  /**
    * 将普通scan转换成base64字符串
    *
    * @param scan
    * @return
    */
  def convertBase64Scan(scan: Scan) = {
    val protoScan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(protoScan.toByteArray)
  }

  /**
    * 从hbase中加载数据
    *
    * @param startTime
    * @param endTime
    * @param sparkSession
    */
  def loadLogDataFromHbase(startTime: Long, endTime: Long, sparkSession: SparkSession) = {
    //初始化扫描器
    val scan = initScan(startTime, endTime)
    //scan转换成base64字符串
    val base64Scan = convertBase64Scan(scan)
    //创建配置文件对象
    val jobConf = new JobConf(base64Scan)
    jobConf.set(TableInputFormat.INPUT_TABLE, LogConstants.LOG_HBASE_TABLE)
    jobConf.set(TableInputFormat.SCAN, base64Scan)

    val tags4LogRDD = sparkSession.sparkContext.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(t2 => {
      val result = t2._2
      val deviceId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes()))
      val albumId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_ALBUM_ID.getBytes()))
      val anchorId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_ANCHOR_ID.getBytes()))
      val playTime = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_PLAY_TIME.getBytes()))
      val accessTime = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME.getBytes()))
      val osName = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_OS_NAME.getBytes()))
      val osVersion = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_OS_VERSION.getBytes()))
      val modelNum = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(), LogConstants.LOG_COLUMNS_NAME_MODEL_NUM.getBytes()))
      val logs = new Logs(accessTime = accessTime, osName = osName, osVersion = osVersion, modelNum = modelNum, deviceId = deviceId, albumId = albumId, anchorId = anchorId, playTime = playTime)
      (deviceId, (Tags4UserInterested.makeTags(logs) ++ Tags4Device.makeTags(logs) ++ Tags4AppBehavior.makeTags(logs)).toList)
    })
      .filter(t2 => t2._2.size > 0)
    tags4LogRDD
  }

  /**
    * 从mysql中加载数据
    *
    * @param tableName
    * @param sparkSession
    * @return
    */
  def loadDataFromMySql(tableName: String, sparkSession: SparkSession) = {
    val url = "jdbc:mysql://192.168.80.8:3306/kugou_report?useUnicode=true&characterEncoding=UTF-8"
    val prop = new Properties()
    //标明使用的数据库连接驱动
    prop.put("driver", "com.mysql.jdbc.Driver")
    prop.put("user", "root")
    prop.put("password", "root")
    //Dataset<Row> ===> DataFrame
    sparkSession.read.jdbc(url, tableName, prop)
  }

  /**
    * 抽取用户兴趣标签，保存到Hive中
    */
  def userPortrait4Interested(reduceRDD: RDD[(String, List[(String, Int)])], sparkSession: SparkSession) = {

    //加载专辑数据Dataset<Row>===>Dataset<(String,String)>
    //导入隐式转换
    import sparkSession.implicits._
    //(albumId,tags)
    val albumData = loadDataFromMySql("dt_album", sparkSession).map(row => (row.getAs[Int]("albumId").toString, row.getAs[String]("tags"))).collect().toMap
    val albumDataBroadCast = sparkSession.sparkContext.broadcast(albumData)

    //加载专辑标签数据
    //(tagId,tagName)
    val tagData = loadDataFromMySql("tag", sparkSession).map(row => (row.getAs[Int]("tagId").toString, row.getAs[String]("tagName"))).collect().toMap
    val tagDataBroadCast = sparkSession.sparkContext.broadcast(tagData)


    val ds = reduceRDD.map(t2 => {
      val deviceId = t2._1
      //对专辑进行处理
      val list = t2._2
      //获取用户感兴趣的专辑id
      val albums = list.filter(x => x._1.contains(TagsConstants.INTERESTED_ALBUM_ID_PREFIX)).sortBy(x => -x._2).map(x => x._1.split("_")(3))
      val interested_album_ids = albums.mkString(",")
      //获取感兴趣的专辑标签Id
      //Array("325,327,346,379,352","325,327,346,379,352")
      //Array(Array(325,327,346,379,352),Array(325,327,346,379,352))
      //Array(325,327,346,379,352)
      val albumsTags = albums.map(abumId => albumDataBroadCast.value.getOrElse(abumId, null)).filter(x => x != null).flatMap(tags => tags.split(",")).distinct

      val interested_album_tags_id = albumsTags.mkString(",")
      //获取专辑标签
      val interested_album_tags_name = albumsTags.map(tagId => tagDataBroadCast.value.getOrElse(tagId, null)).filter(x => x != null).mkString(",")
      //主播id
      val interested_anchor_ids = list.filter(x => x._1.contains(TagsConstants.INTERESTED_ANCHOR_ID_PREFIX)).sortBy(x => -x._2).map(x => x._1.split("_")(3)).mkString(",")
      //
      val interest_program_time_level = list.filter(x => x._1.contains(TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX)).sortBy(x => -x._2).map(x => {
        val fields = x._1.split("_")
        fields(4) + "_" + fields(5)
      }).mkString(",")
      UserInterested(deviceId, interested_album_ids, interested_album_tags_id, interested_album_tags_name, interested_anchor_ids, interest_program_time_level)
    }).toDS().createOrReplaceTempView("user_interested")
    val dataFrame = sparkSession.sql("select * from user_interested where interested_album_tags_name !='' ")

    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable("kugou_bi.user_interested")

  }

  /**
    * 抽取用户使用app行为标签，并保存到Hive表中
    *
    * @param reduceRDD
    * @param sparkSession
    */
  def userPortrait4AppBehavior(reduceRDD: RDD[(String, List[(String, Int)])], sparkSession: SparkSession) = {
    val appBehaviorRDD = reduceRDD.map(t2 => {
      val deviceId = t2._1
      val list = t2._2
      //使用时间段
      var use_time_period = list.filter(x => x._1.contains(TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX)).sortBy(x => -x._2).map(x => {
        val fields = x._1.split("_")
        (fields(4) + "_" + fields(5))
      }).mkString(",")
      //手机型号
      val phone_model = list.filter(x => x._1.contains(TagsConstants.DEVICE_MODEL_NUM_PREFIX)).sortBy(x => -x._2).map(x => x._1.split("_")(3)).mkString(",")
      val phone_os_name = list.filter(x => x._1.contains(TagsConstants.DEVICE_OS_NAME_PREFIX)).sortBy(x => -x._2).map(x => x._1.split("_")(3)).mkString(",")
      val phone_os_version = list.filter(x => x._1.contains(TagsConstants.DEVICE_OS_VERSION_PREFIX)).sortBy(x => -x._2).map(x => x._1.split("_")(3)).mkString(",")
      AppBehavior(deviceId, use_time_period, phone_model, phone_os_name, phone_os_version)
    })

    import sparkSession.implicits._
    appBehaviorRDD.toDS().createOrReplaceTempView("app_behavior")
    import sparkSession.sql
    sql("select * from app_behavior").write.mode(SaveMode.Overwrite).saveAsTable("kugou_bi.app_behavior")
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    //验证日期是否正确
    validateArgs(args)
    val startTime = Utils.parseDateToLong(args(0), "yyyy-MM-dd")
    val endTime = Utils.caculateDate(startTime, 7)
    val sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      //激活spark操作hive相关功能
      .enableHiveSupport().getOrCreate()

    //从hbase中加载数据
    /**
      * (865315030334771,List((interested_album_id_16431,1), (interested_program_time_level_10_20,1), (interested_anchor_id_11569,1)))
      * (865315030334771,List((interested_album_id_16431,1), (interested_anchor_id_11569,1)))
      */
    val tags4LogRDD = loadLogDataFromHbase(startTime, endTime, sparkSession)
    //reduceByKey(_+_)
    //(deviceId,List)
    val reduceRDD = tags4LogRDD.reduceByKey {
      case (list0, list1) => {
        //List((interested_album_id_16431,1), (interested_program_time_level_10_20,1), (interested_anchor_id_11569,1),(interested_album_id_16431,1), (interested_anchor_id_11569,1))
        (list0 ++ list1)
          //List(
          // (interested_album_id_16431,List((interested_album_id_16431,1),(interested_album_id_16431,1)))
          // (interested_anchor_id_11569,List((interested_anchor_id_11569,1), (interested_anchor_id_11569,1)))
          // (interested_program_time_level_10_20,1),List( (interested_program_time_level_10_20,1)))
          // )
          .groupBy(x => x._1)
          //List(
          // (interested_album_id_16432,3)
          // (interested_album_id_16431,2)
          // (interested_anchor_id_11569,2)
          // (interested_program_time_level_10_20,1))
          // )
          .map(g => (g._1, g._2.map(x => x._2).sum)).toList
      }
    }

    //抽取用户兴趣标签，并保存到Hive表中
    userPortrait4Interested(reduceRDD, sparkSession)

    //抽取用户使用app行为标签，并保存到Hive表中
    userPortrait4AppBehavior(reduceRDD, sparkSession)


    sparkSession.stop()
  }
}
