package com.sparkutils.flow

import com.sparkutils.quality.Id
import frameless._
import org.apache.spark.sql.Encoder
import shapeless.ops.hlist.IsHCons
import shapeless.{HList, LabelledGeneric, Lazy}

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

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
      s.toLowerCase.replaceAll("_","").replaceAll("-","") match {
        case CollectRunnerName | "collectrunner" => Collect
        case EngineRunnerName | "ruleengine" | "ruleenginerunner" => Engine
        case FolderRunnerName | "fold" | "folderrunner" => Folder
        case DQRunnerName | "dqrulerunner" | "rulerunner" => DQ
        case NoOpRunnerName => NoOp
        case _ => CustomRunnerEngine(s)
      }
  )

  implicit val durationEnc =
    Injection[Duration, String]( _.toString , s => Duration(s) )

  implicit val typedOperationEnc = TypedEncoder[OperationRow]
  implicit val typedOperationExpEnc: Encoder[OperationRow] = TypedExpressionEncoder[OperationRow]

  implicit val typedStepEnc = TypedEncoder[StepRow]
  implicit val typedStepExpEnc: Encoder[StepRow] = TypedExpressionEncoder[StepRow]

  implicit def typedFlowRuleGroupEnc[T: TypedEncoder: FlowRuleGroupProcessing, G <: HList, H <: HList](implicit
      i0: LabelledGeneric.Aux[FlowRuleGroup[T], G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons[H],
      i3: Lazy[RecordEncoderFields[H]],
      i5: ClassTag[FlowRuleGroup[T]]
    ): TypedEncoder[FlowRuleGroup[T]] = {
      TypedEncoder.usingDerivation[FlowRuleGroup[T], G, H]
    }

  implicit def typedFlowRuleGroupExpEnc[FG: TypedEncoder: FlowRuleGroupProcessing]: Encoder[FlowRuleGroup[FG]] =
    TypedExpressionEncoder[FlowRuleGroup[FG]]

  implicit def typedFlowEnc[T: TypedEncoder: FlowRuleGroupProcessing, G <: HList, H <: HList](implicit
      i0: LabelledGeneric.Aux[FlowRow[T], G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons[H],
      i3: Lazy[RecordEncoderFields[H]],
      i5: ClassTag[FlowRow[T]]
    ): TypedEncoder[FlowRow[T]] = {
      TypedEncoder.usingDerivation[FlowRow[T], G, H]
    }
  implicit def typedFlowExpEnc[FG: TypedEncoder: FlowRuleGroupProcessing]: Encoder[FlowRow[FG]] =
    TypedExpressionEncoder[FlowRow[FG]]

  implicit def typedFullFlowEnc[FG: TypedEncoder: FlowRuleGroupProcessing, RP: TypedEncoder: RuleSuiteStorageType,
    G <: HList, H <: HList](implicit
      i0: LabelledGeneric.Aux[FullFlow[FG, RP], G],
      i1: DropUnitValues.Aux[G, H],
      i2: IsHCons[H],
      i3: Lazy[RecordEncoderFields[H]],
      i5: ClassTag[FullFlow[FG, RP]]
    ): TypedEncoder[FullFlow[FG, RP]] = {
      TypedEncoder.usingDerivation[FullFlow[FG, RP], G, H]
    }

  implicit def typedFullFlowExpEnc[FG: TypedEncoder: FlowRuleGroupProcessing, RP: TypedEncoder: RuleSuiteStorageType]:
    Encoder[FullFlow[FG, RP]] = TypedExpressionEncoder[FullFlow[FG, RP]]

}
