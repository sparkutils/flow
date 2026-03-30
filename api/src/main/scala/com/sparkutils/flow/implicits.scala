package com.sparkutils.flow

import frameless._

object implicits {

  implicit val resultApproachEnc = Injection[ResultApproach, String](_.getClass.getSimpleName, {
    case "AsIs" => AsIs
    case "ExpandNested" => ExpandNested
    case "MergeFields" => MergeFields
    case "StarOnly" => StarOnly
    case "OutputFieldOnly" => OutputFieldOnly
  })

}
