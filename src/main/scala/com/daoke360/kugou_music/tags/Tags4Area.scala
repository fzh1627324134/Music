package com.daoke360.kugou_music.tags

import com.daoke360.kugou_music.bean.Logs
import com.daoke360.kugou_music.constants.TagsConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable



object Tags4Area extends Tags {
  /**
    * 打标签的方法
    *
    * @param args
    */
  override def makeTags(args: Any*) = {
    val areaMap = mutable.Map[String, Int]()
    if (args.length > 0) {
      val logs = args(0).asInstanceOf[Logs]
      if (StringUtils.isNotBlank(logs.country)) {
        areaMap += (TagsConstants.AREA + logs.country + "_" + logs.province + "_" + logs.city -> 1)
      }
    }
    areaMap

  }
}
