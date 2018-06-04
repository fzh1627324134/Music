package com.daoke360.kugou_music.bean

/**
  * os_n:操作系统名称
  * os_v:操作系统版本
  * ip:ip地址
  * country:国家
  * province:省份
  * city:城市
  * albumId:专辑id
  * programId:节目id
  * playTime：播放时长
  * access_time：日志时间
  * *
  * behaviorKey：行为标识Key
  * behaviorData：用户行为数据
  * *
  *
  * modelNum：手机型号
  * request_type：请求方式 GET 或POST
  * anchorId:主播id
  * zongKey：app区域信息
  * behavior:用户行为标识
  */
class Logs(
            var deviceId: String = null, //设备id
            var ip: String = null, //ip地址
            var country: String = null, //国家
            var province: String = null, //省份
            var city: String = null, //城市
            var albumId: String = null, //专辑id
            var programId: String = null, //节目id
            var anchorId: String = null, //主播id
            var zongKey: String = null, //app区域信息
            var playTime: String = null, //播放时长
            var accessTime: String = null, //访问时间
            var behaviorKey: String = null, //行为标识Key
            var behaviorData: String = null, //用户行为数据
            var modelNum: String = null, //手机型号
            var requestType: String = null, //请求方式
            var osName: String = null, //请求方式
            var osVersion: String = null //请求方式
          ) {
}

