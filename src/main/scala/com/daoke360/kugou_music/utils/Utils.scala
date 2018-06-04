package com.daoke360.kugou_music.utils

import java.text.SimpleDateFormat
import java.util.{Base64, Calendar, Locale}
import java.util.regex.Pattern
import java.math.BigDecimal

import scala.math.BigDecimal.RoundingMode

object Utils {

  /**
    * 四舍五入
    *
    * @param doubleValue
    * @param scale
    * @return
    */
  def getScale(doubleValue: Double, scale: Int) = {
    val bigDecimal = new BigDecimal(doubleValue)
    bigDecimal.setScale(scale, RoundingMode.HALF_UP)
  }

  /**
    * 日期加减
    *
    * @param longTime
    * @param day
    * @return
    */
  def caculateDate(longTime: Long, day: Int) = {
    val calender = Calendar.getInstance()
    calender.setTimeInMillis(longTime)
    calender.add(Calendar.DAY_OF_MONTH, day)
    calender.getTimeInMillis
  }

  /**
    * 验证是否是一个数字
    *
    * @param numberString
    * @return
    */
  def validateNumber(numberString: String) = {
    val reg = "[0-9]{1,}"
    val pattern = Pattern.compile(reg)
    pattern.matcher(numberString).matches()
  }

  /**
    * 验证ip是否符合ip格式
    *
    * @param ip
    */
  def validateIP(ip: String) = {
    val reg = "((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))"
    val pattern = Pattern.compile(reg)
    pattern.matcher(ip).matches()
  }

  /**
    * 将日志服务器时间转换成时间戳
    *
    * @param accessTime 24/Apr/2018:03:14:21 +0800
    */
  def parseLogServerTimeToLong(accessTime: String) = {
    val simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0800", Locale.ENGLISH)
    val date = simpleDateFormat.parse(accessTime)
    date.getTime
  }

  /**
    * base64解码
    *
    * @param base64EndcodeString
    */
  def base64Decode(base64EndcodeString: String) = {
    Base64.getDecoder.decode(base64EndcodeString)
  }

  /**
    * 验证日期是否是 yyyy-MM-dd
    *
    * @param date
    */
  def validateDate(date: String) = {
    val reg = "((((19|20)\\d{2})-(0?(1|[3-9])|1[012])-(0?[1-9]|[12]\\d|30))|(((19|20)\\d{2})-(0?[13578]|1[02])-31)|(((19|20)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))-0?2-29))$"
    val pattern = Pattern.compile(reg)
    pattern.matcher(date).matches()
  }

  /**
    * 将时间戳转换成指定格式的日期
    *
    * @param longTime
    * @param pattern
    * @return
    */
  def formatDate(longTime: Long, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    sdf.format(calendar.getTime)
  }

  /**
    * 将字符串时间转换成 long类型的时间戳
    *
    * @param strDate 2017-10-23
    * @param pattern yyyy-MM-dd
    */
  def parseDateToLong(strDate: String, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.parse(strDate).getTime
  }

}
