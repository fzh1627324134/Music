package com.daoke360.kugou_music.caseclass

case class IPRule(var startIP: Long, var endIP: Long, var country: String, var province: String, var city: String) extends Serializable {
}
