package com.daoke360.kugou_music.tags

import com.daoke360.kugou_music.bean.Logs
import com.daoke360.kugou_music.constants.TagsConstants
import com.daoke360.kugou_music.utils.Utils

import scala.collection.mutable


object Tags4AppBehavior extends Tags {
  /**
    * 打标签的方法
    *
    * @param args
    */
  override def makeTags(args: Any*): mutable.Map[String, Int] = {
    val behaviorMap = mutable.Map[String, Int]()
    if (args.length > 0) {
      val logs = args(0).asInstanceOf[Logs]
      val hour = Utils.formatDate(logs.accessTime.toLong, "HH").toInt
      if (hour >= 4 && hour < 7) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "4_7" -> 1)
      }
      if (hour >= 7 && hour < 9) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "7_9" -> 1)
      }
      if (hour >= 9 && hour < 12) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "9_12" -> 1)
      }
      if (hour >= 12 && hour < 13) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "12_13" -> 1)
      }
      if (hour >= 13 && hour < 18) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "13_18" -> 1)
      }
      if (hour >= 18 && hour < 20) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "18_20" -> 1)
      }
      if (hour >= 20 && hour < 23) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "20_23" -> 1)
      }
      if (hour == 23 || (hour >= 0 && hour < 4)) {
        behaviorMap += (TagsConstants.BEHAVIOR_USE_TIME_PERIOD_PREFIX + "23_4" -> 1)
      }
    }

    behaviorMap
  }
}
