package com.daoke360.kugou_music.tags

import com.daoke360.kugou_music.bean.Logs
import com.daoke360.kugou_music.constants.TagsConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

object Tags4UserInterested extends Tags {
  /**
    * 打标签的方法
    *
    * @param args
    */
  override def makeTags(args: Any*) = {
    val userInterestedMap = mutable.Map[String, Int]()
    if (args.length > 0) {
      val logs = args(0).asInstanceOf[Logs]
      if (StringUtils.isNotBlank(logs.albumId)) {
        //("interested_album_id_346",1)
        userInterestedMap += (TagsConstants.INTERESTED_ALBUM_ID_PREFIX + logs.albumId -> 1)
      }
      if (StringUtils.isNotBlank(logs.anchorId)) {
        userInterestedMap += (TagsConstants.INTERESTED_ANCHOR_ID_PREFIX + logs.anchorId -> 1)
      }

      if (StringUtils.isNotBlank(logs.playTime)) {
        val playTimeLong = logs.playTime.toInt / 60
        if (playTimeLong > 0 && playTimeLong <= 5) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "0_5" -> 1)
        } else if (playTimeLong > 5 && playTimeLong <= 10) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "5_10" -> 1)
        } else if (playTimeLong > 10 && playTimeLong <= 20) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "10_20" -> 1)
        } else if (playTimeLong > 20 && playTimeLong <= 30) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "20_30" -> 1)
        } else if (playTimeLong > 30) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "30_plus" -> 1)
        }
      }

    }
    userInterestedMap
  }
}
