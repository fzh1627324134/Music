package com.daoke360.kugou_music.tags

import scala.collection.mutable


trait Tags {

  /**
    * 打标签的方法
    *
    * @param args
    */
  def makeTags(args: Any*):mutable.Map[String,Int]
}
