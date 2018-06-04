package com.daoke360.kugou_music.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}


object JedisUtils {
  val config = new JedisPoolConfig()
  val jedisPool = new JedisPool(config, "mini1", 6379, 0)

  def getConnection() = {
    jedisPool.getResource
  }

  /**
    * 向 redis中写入hash数据类型的数据
    *
    * @param key
    * @param field
    * @param value
    */
  def hset(key: String, field: String, value: String) = {
    val client = getConnection()
    client.hset(key, field, value)
    client.close()
  }


}
