package com.daoke360.kugou_music.caseclass

import com.daoke360.kugou_music.constants.GlobalConstants

case class RegionInfo(var country:String=GlobalConstants.DEFAULT_VALUE,var province:String=GlobalConstants.DEFAULT_VALUE,var city:String=GlobalConstants.DEFAULT_VALUE) {
}
