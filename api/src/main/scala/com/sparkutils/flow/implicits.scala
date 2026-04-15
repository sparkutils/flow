package com.sparkutils.flow

import frameless._

object implicits {

  implicit val resultApproachEnc = Injection[ResultApproach, String](
    _.getClass.getSimpleName.dropRight(1),// drop the module $
    {
    case "AsIs" => AsIs
    case "ExpandNested" => ExpandNested
    case "MergeFields" => MergeFields
    case "StarOnly" => StarOnly
    case "OutputFieldOnly" => OutputFieldOnly
  })

  implicit val typedStep = TypedExpressionEncoder[StepRow]
  implicit val typedFlow = TypedExpressionEncoder[FlowRow]
  implicit val fullFlow = TypedExpressionEncoder[FullFlow]

}
