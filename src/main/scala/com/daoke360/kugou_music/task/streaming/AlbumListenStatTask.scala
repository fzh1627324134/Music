package com.daoke360.kugou_music.task.streaming

import com.daoke360.kugou_music.common.LogAnalysis
import com.daoke360.kugou_music.constants.LogConstants
import com.daoke360.kugou_music.enum.BehaviorKeyEnum
import com.daoke360.kugou_music.utils.JedisUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * 启动kafka命令
  * /opt/apps/kafka/bin/kafka-server-start.sh  -daemon /opt/apps/kafka/config/server.properties
  * ##查看所有topic##
  * /opt/apps/kafka/bin/kafka-topics.sh --list --zookeeper  mini1:2181
  * ##创建topic##
  * /opt/apps/kafka/bin/kafka-topics.sh --create --zookeeper mini1:2181 --replication-factor 1 --partitions 3 --topic kugou_music_log
  * ##客户日志生产者##
  * /opt/apps/kafka/bin/kafka-console-producer.sh --broker-list mini1:9092 --topic kugou_music_log
  * ##客户日志消费者##
  * /opt/apps/kafka/bin/kafka-console-consumer.sh --zookeeper mini1:2181  --topic kugou_music_log
  */
object AlbumListenStatTask {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "mini1:2181,mini2:2181,mini3:2181",
      //消费者组id
      "group.id" -> "kugou_consumer_group",
      //提交消费偏移量的时间间隔
      "auto.commit.interval.ms" -> "3000",
      //从最大消息偏移量开始消费
      "auto.offset.reset" -> "largest"
    )

    val kafkaInputDstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map("kugou_music_log" -> 3), StorageLevel.MEMORY_ONLY_SER)
      .map(_._2)

    kafkaInputDstream.map(logText => {
      val map = LogAnalysis.analysisLog(logText, null)
      map
    }).filter(map =>
      map != null
        && map.contains(LogConstants.LOG_COLUMNS_NAME_BEHAVIOR_KEY)
        && map(LogConstants.LOG_COLUMNS_NAME_BEHAVIOR_KEY).equals(BehaviorKeyEnum.DFSJ400.toString)
    ).map(map => {
      val albumId = map(LogConstants.LOG_COLUMNS_NAME_ALBUM_ID)
      val programId = map(LogConstants.LOG_COLUMNS_NAME_PROGRAM_ID)
      ((albumId, programId), 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partitionIter => {
          val jedisClient = JedisUtils.getConnection()
          partitionIter.foreach(t2 => {
            val albumId = t2._1._1
            val programId = t2._1._2
            val playCount = t2._2
            val key = "albumId_"
            val programField = "programId_"
            val albumIdField = "album_play_count"
            //专辑播放次数需要进行累加
            jedisClient.hincrBy(key + albumId, albumIdField, playCount)
            jedisClient.hincrBy(key + albumId, programField + programId, playCount)
          })
          jedisClient.close()
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
