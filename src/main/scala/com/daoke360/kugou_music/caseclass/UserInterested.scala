package com.daoke360.kugou_music.caseclass

case class UserInterested(var user_id: String, var interested_album_ids: String,
                          var interested_album_tags_id: String, var interested_album_tags_name: String,
                          var interested_anchor_ids: String, var interest_program_time_level: String)
