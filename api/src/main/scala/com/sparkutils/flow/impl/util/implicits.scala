package com.sparkutils.flow.impl.util

import com.sparkutils.flow.{AsIs, Collect, CollectRunnerName, CustomApproach, CustomRunnerEngine, DQ, DQRunnerName, Engine, EngineRunnerName, FlowRow, FlowRuleGroup, Folder, FolderRunnerName, FullFlow, MergeFields, OperationRow, OutputFieldOnly, OutputFieldsOnly, ResultApproach, Runner, StarOnly, StepRow}
import frameless._

import scala.concurrent.duration.Duration

object implicits {

  implicit val resultApproachEnc = Injection[ResultApproach, String](
    {
      case CustomApproach(s) => s
      case a => a.getClass.getSimpleName.dropRight(1)// drop the module $
    },
    s => s.toLowerCase match {
      case "asis" => AsIs
      case "mergefields" => MergeFields
      case "staronly" => StarOnly
      case "outputfieldonly" => OutputFieldOnly
      case "outputfieldsonly" => OutputFieldsOnly
      case _ => CustomApproach(s)
    }
  )

  implicit val runnerEnc = Injection[Runner, String](
    {
      case CustomRunnerEngine(s) => s
      case a => a.getClass.getSimpleName.dropRight(1)// drop the module $
    }, s =>
      s.toLowerCase.replaceAll("_","") match {
        case CollectRunnerName | "collectrunner" => Collect
        case EngineRunnerName | "ruleengine" | "ruleenginerunner" => Engine
        case FolderRunnerName | "fold" | "folderrunner" => Folder
        case DQRunnerName | "dqrulerunner" | "rulerunner" => DQ
        case _ => CustomRunnerEngine(s)
      }
  )

  implicit val durationEnc =
    Injection[Duration, String]( _.toString , s => Duration(s) )

  implicit val typedOperation = TypedExpressionEncoder[OperationRow]
  implicit val typedStep = TypedExpressionEncoder[StepRow]
  implicit val typedFlow = TypedExpressionEncoder[FlowRow]
  implicit val fullFlow = TypedExpressionEncoder[FullFlow]

}
