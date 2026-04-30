package com.sparkutils.flow

import com.sparkutils.flow.impl.util.Utils.defaultCombinedFolder
import com.sparkutils.quality
import com.sparkutils.quality.implicits.combinedRuleSuiteRowTypedExpEnc
import com.sparkutils.quality.{CombinedRuleSuiteRows, GroupRuleId, Id, LambdaFunctionRow, MapRow, OutputExpressionRow, RuleRow, RuleSuite, RuleSuiteParam, RuleSuiteRow, VersionedId, ViewRow, register_rule_suite_group_variable, rule_suite, rule_suite_from, toDS, toLambdaDS, toOutputExpressionDS, toRuleSuiteRow}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, ShimUtils, SparkSession}
import org.apache.spark.sql.functions.{col, lit, struct}

import scala.concurrent.duration.Duration
import scala.util.Try

@SerialVersionUID(1L)
case class OperationRow(function: Runner, fieldName: Option[String], resultApproach: ResultApproach,
                        combineAuditWith: Option[Set[String]] = None) extends Serializable

@SerialVersionUID(1L)
case class StepRow(flowId: Int, flowVersion: Int, name: String, dependencies: scala.collection.immutable.Set[String],
                   ruleSuiteId: Int, ruleSuiteVersion: Int,
                   operation: OperationRow, options: Map[String, String], data: StepData) extends StepLike {

  override def function: Runner = operation.function
}

@SerialVersionUID(1L)
case class FlowRuleGroup[T](ruleGroupName: String, ruleGroup: Seq[T]) extends Serializable

@SerialVersionUID(1L)
case class FlowRow[FG](flowId: Int, flowVersion: Int, flowAuditColName: String, duration: Duration,
                       flowRuleGroup: Option[FlowRuleGroup[FG]]) extends Serializable

@SerialVersionUID(1L)
case class FlowDataSets[FG](steps: Dataset[StepRow], flows: Dataset[FlowRow[FG]], ruleRows: Dataset[RuleRow],
                        lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]] = None,
                        outputExpressionRows: Option[Dataset[OutputExpressionRow]] = None,
                        globalLambdaSuites: Option[Dataset[Id]] = None,
                        globalOutputExpressionSuites: Option[Dataset[Id]] = None,
                        ruleSuites: Option[Dataset[RuleSuiteRow]] = None,
                        viewRows: Option[Dataset[ViewRow]] = None,
                        mapRows: Option[Dataset[MapRow]] = None,
                       ) extends Serializable {

  def combined: Dataset[CombinedRuleSuiteRows] =
    quality.combine(ruleRows, lambdaFunctionRows, outputExpressionRows, globalLambdaSuites,
      globalOutputExpressionSuites, ruleSuites)
}

@SerialVersionUID(1L)
case class FullStep[RP](step: StepRow, ruleSuite: RP, initConfiguration: StepInitConfiguration) extends Serializable

@SerialVersionUID(1L)
case class FullFlow[FG, RP](flowRow: FlowRow[FG], steps: Seq[FullStep[RP]]) extends Serializable

/**
 * Represents the data needed to create a Flow
 * @param flowRow parameters used to create a flow
 * @param steps the steps for this flow
 */
@SerialVersionUID(1L)
case class FlowData[FG, RP](flowRow: FlowRow[FG], steps: Seq[Step[RP]]) extends Serializable

trait Serialisation {

  def loadSteps(dataFrame: DataFrame, flowId: Column, flowVersion: Column, name: Column, dependencies: Column,
                ruleSuiteId: Column, ruleSuiteVersion: Column, operation: Column,
                options: Column, data: Column): Dataset[StepRow] = {
    import frameless._
    import implicits._
    import com.sparkutils.quality.implicits._

    dataFrame.select(
      flowId.as("flowId"), flowVersion.as("flowVersion"), name.as("name"), dependencies.as("dependencies"),
      ruleSuiteId.as("ruleSuiteId"), ruleSuiteVersion.as("ruleSuiteVersion"),
      operation.as("operation"), options.as("options"), data.as("data")
    ).as[StepRow]
  }

  def loadSteps(dataFrame: DataFrame): Dataset[StepRow] =
    loadSteps(dataFrame, col("flowId"), col("flowVersion"), col("name"), col("dependencies"),
      col("ruleSuiteId"), col("ruleSuiteVersion"), col("operation"),
      col("options"), col("data"))

  // TODO versioned impl

  /**
   * Provides a single struct representation for a Flow, suitable for json usage encoding RuleSuites directly within
   * a Step
   * @param sparkSession
   * @param flow
   * @return
   */
  def toFullFlow[FG: FlowRuleGroupProcessing](
    sparkSession: SparkSession, flow: FlowT[FG, RuleSuite])(
    implicit ffenc: Encoder[FullFlow[FG, CombinedRuleSuiteRows]], fgenc: Encoder[FlowRuleGroup[FG]],
    operation: FlowGroupAndOperationType[FG, RuleSuiteFromDataset.type], idParam: RuleSuiteTypeParam[RuleSuite],
    storageParam: RuleSuiteStorageType[CombinedRuleSuiteRows]):
    Dataset[FullFlow[FG, CombinedRuleSuiteRows]] =
      toFullFlow[FG, RuleSuiteFromDataset.type](RuleSuiteFromDataset)(sparkSession, flow)

  /**
   * Provides a single struct representation for a Flow, suitable for json usage, allowing processing
   * [[OperationProcessing]] to dictate how the RuleSuites should be saved (either in-line with
   * the Step's or as part of the [[FlowRuleGroup]]
   * @param sparkSession
   * @param flow
   * @return
   */
  def toFullFlow[FG: FlowRuleGroupProcessing, OP <: OperationProcessing](processing: OP)(
    sparkSession: SparkSession, flow: FlowT[FG, OP#ResType])(
    implicit ffenc: Encoder[FullFlow[FG, OP#StorageType]], fgenc: Encoder[FlowRuleGroup[FG]],
    operation: FlowGroupAndOperationType[FG, OP], idParam: RuleSuiteTypeParam[OP#ResType],
    storageParam: RuleSuiteStorageType[OP#StorageType]):
    Dataset[FullFlow[FG, OP#StorageType]] = {

    import frameless._
    import implicits._

    import com.sparkutils.quality.implicits._

    import sparkSession.implicits._

    Seq(
      FullFlow(toFlowRow(flow),
        flow.steps.map{ step =>
          FullStep(
            stepRow[FG, OP](flow, step),
            operation.writableRuleSuite(step.operation.ruleSuite),
            step.initConfiguration
          )
        })
    ).toDS()
  }

  protected def toFlowRow[FG: FlowRuleGroupProcessing, RP](flow: FlowT[FG, RP])(implicit fgenc: Encoder[FlowRuleGroup[FG]]) = {
    FlowRow[FG](flowId = flow.flowId.id, flowVersion = flow.flowId.version,
      flowAuditColName = flow.flowAuditColName, duration = flow.duration, flowRuleGroup = flow.flowRuleGroup)
  }

  protected def flowFilter(flowId: VersionedId, prefix: String = ""): String =
    s"${prefix}flowID = ${flowId.id} and ${prefix}flowVersion = ${flowId.version} "

  /**
   * Load a Flow from a FullFlow dataset, defaulting to loading full RuleSuites embedded within Steps
   * @param dataset
   * @param flowId
   * @return
   */
  def fromFullFlow[FG: FlowRuleGroupProcessing](
    dataset: Dataset[FullFlow[FG, CombinedRuleSuiteRows]], flowId: VersionedId)(
      implicit rstp: RuleSuiteTypeParam[RuleSuite], operation: FlowGroupAndOperationType[FG, RuleSuiteFromDataset.type]
    ): FlowData[FG, RuleSuite] =
    fromFullFlow(RuleSuiteFromDataset)(dataset, flowId)

  /**
   * Load a Flow from a FullFlow dataset, allowing flexible RuleSuite storage locations
   * @param dataset
   * @param flowId
   * @return
   */
  def fromFullFlow[FG: FlowRuleGroupProcessing, RP <: OperationProcessing](processing: RP)(
    dataset: Dataset[FullFlow[FG, RP#StorageType]], flowId: VersionedId)(
      implicit rstp: RuleSuiteTypeParam[RP#ResType], operation: FlowGroupAndOperationType[FG, RP]
    ): FlowData[FG, RP#ResType] = {
    val r = dataset.filter(flowFilter(flowId, "flowRow.")).collect()
    if (r.isEmpty) {
      throw FlowException(s"Flow $flowId could not be loaded fromFullFlow does not contain that flow")
    }

    val full = r.head
    // map of id's to default the folder
    val stepIds =
      full.steps.map{
        fullStep =>
          import fullStep._
          Id(step.ruleSuiteId, step.ruleSuiteVersion) -> step }.toMap

    val rules = (
        full.flowRow.flowRuleGroup.map(implicitly[FlowRuleGroupProcessing[FG]].combinedRuleSuiteRows)
          .getOrElse(Seq.empty) ++
        full.steps.flatMap(s => operation.toCombinedRows(s.ruleSuite))
      ).map(defaultCombinedFolder(stepIds))

    import dataset.sparkSession.implicits._
    val variableName =
      full.flowRow.flowRuleGroup.fold(
        register_rule_suite_group_variable(rules.toDS())
      )( g =>
        register_rule_suite_group_variable(rules.toDS(), g.ruleGroupName)
      )

    FlowData(full.flowRow,
      full.steps.map{
        fullStep =>
          import fullStep._
          val rid = Id(step.ruleSuiteId, step.ruleSuiteVersion)
          val rs = operation.fromFullRow(rules, variableName, full.flowRow, fullStep, rid, ruleSuite)

          Step(name = step.name, dependencies = step.dependencies, operation =
            Operation(rs, step.operation.function, step.operation.resultApproach,
              step.operation.fieldName, step.operation.combineAuditWith),
            initConfiguration = initConfiguration,
            data = step.data, step.options)
      }
    )
  }

  /**
   * Converts to FlowDataSet using Step RuleSuites encoded as part of the top level FlowDataSets instead of [[FlowRuleGroup]]
   */
  def toDatasets[FG: FlowRuleGroupProcessing](
    sparkSession: SparkSession, flow: FlowT[FG, RuleSuite])(
      implicit ffenc: Encoder[FullFlow[FG, CombinedRuleSuiteRows]], fgenc: Encoder[FlowRuleGroup[FG]], frenc: Encoder[FlowRow[FG]],
      rstp: RuleSuiteTypeParam[RuleSuite], operation: FlowGroupAndOperationType[FG, RuleSuiteFromDataset.type]
    ): FlowDataSets[FG] =
    toDatasets(RuleSuiteFromDataset)(sparkSession, flow)

  /**
   * Converts a Flow built with RuleSuites to it's underlying datasets, global output and lambdas are not supported
   * (they are treated as part of the ds directly).
   *
   * Use the [[convertToIds]] function with a FullFlow to convert if Id based Flows are desired
   */
  def toDatasets[FG: FlowRuleGroupProcessing, RP <: OperationProcessing](processing: RP)(
    sparkSession: SparkSession, flow: FlowT[FG, RP#ResType])(
      implicit ffenc: Encoder[FullFlow[FG, RP#StorageType]], fgenc: Encoder[FlowRuleGroup[FG]], frenc: Encoder[FlowRow[FG]],
      rstp: RuleSuiteTypeParam[RP#ResType], operation: FlowGroupAndOperationType[FG, RP]
    ): FlowDataSets[FG] = {

    import frameless._
    import implicits._

    import com.sparkutils.quality.implicits._

    import sparkSession.implicits._

    val ruleSuites = flow.steps.flatMap(s => operation.toRuleSuite(flow, s.operation.ruleSuite ))

    val suiteRows = ruleSuites.map(s => toRuleSuiteRow(s))
    val defaultOutputRows = suiteRows.flatMap(_._2)

    FlowDataSets(
      flow.steps.map{ step =>
        stepRow(flow, step)}.toDS(),
      Seq(toFlowRow(flow)).toDS(),
      ruleSuites.foldLeft(sparkSession.emptyDataset[RuleRow]){ case (cur, s) =>
        toDS(s) union cur
      },
      Some(ruleSuites.foldLeft(sparkSession.emptyDataset[LambdaFunctionRow]){ case (cur, s) =>
        toLambdaDS(s) union cur
      }),
      Some(ruleSuites.foldLeft(sparkSession.emptyDataset[OutputExpressionRow]){ case (cur, s) =>
        toOutputExpressionDS(s) union cur
      } union defaultOutputRows.toDS()),
      ruleSuites = Some(suiteRows.map(_._1).toDS()),
      viewRows = Some(flow.steps.foldLeft(sparkSession.emptyDataset[ViewRow]){ case (cur, s) =>
         s.initConfiguration.viewConfig.toDS() union cur
      }),
      mapRows = Some(flow.steps.foldLeft(sparkSession.emptyDataset[MapRow]){ case (cur, s) =>
        s.initConfiguration.mapConfig.toDS() union cur
      })
    )
  }

  protected def stepRow[FG, OP <: OperationProcessing](flow: FlowT[FG, OP#ResType], step: Step[OP#ResType])(
    implicit rp: RuleSuiteTypeParam[OP#ResType]) = {
    StepRow(flowId = flow.flowId.id, flowVersion = flow.flowId.version, name = step.name,
      dependencies = step.dependencies,
      ruleSuiteId = rp.id(step.operation.ruleSuite).id, ruleSuiteVersion = rp.id(step.operation.ruleSuite).version,
      data = step.data,
      operation = OperationRow(step.operation.function, step.operation.fieldName,
        step.operation.resultApproach, step.operation.combineAuditWith),
      options = step.options)
  }

  /**
   * Default to using embedded RuleSuite's loaded from the flowDataSets, with optional FlowRuleGroup handling
   * @param sparkSession
   * @param flowDataSets
   * @param flowId
   * @param fgenc
   * @param fgP
   * @param operation
   * @tparam FG
   * @return
   */
  def fromDatasets[FG](sparkSession: SparkSession, flowDataSets: FlowDataSets[FG], flowId: VersionedId)(
    implicit fgenc: Encoder[FlowRuleGroup[FG]], fgP: FlowRuleGroupProcessing[FG],
     operation: FlowGroupAndOperationType[FG, RuleSuiteFromDataset.type] ): FlowData[FG, RuleSuite] =
    fromDatasets[FG, RuleSuiteFromDataset.type](RuleSuiteFromDataset)(sparkSession: SparkSession,
      flowDataSets: FlowDataSets[FG], flowId: VersionedId)

  /**
   * Based on processing loads rules from either [[FlowDataSets.combined]] or rows provided from [[FlowDataSets.flows]]
   * @param processing
   * @param sparkSession
   * @param flowDataSets
   * @param flowId
   * @param fgenc
   * @param fgP
   * @param rstp
   * @param operation
   * @tparam FG
   * @tparam RP
   * @return
   */
  def fromDatasets[FG, RP <: OperationProcessing](processing: RP)(
      sparkSession: SparkSession, flowDataSets: FlowDataSets[FG], flowId: VersionedId)(
      implicit fgenc: Encoder[FlowRuleGroup[FG]], fgP: FlowRuleGroupProcessing[FG],
      rstp: RuleSuiteTypeParam[RP#ResType], operation: FlowGroupAndOperationType[FG, RP]
    ): FlowData[FG, RP#ResType] = {
    import flowDataSets._

    import com.sparkutils.quality.implicits._


    val flowSteps = loadSteps(steps.toDF()).filter(flowFilter(flowId))
    val sourceSteps = flowSteps.collect()
    val stepIds = sourceSteps.map(s => Id(s.ruleSuiteId, s.ruleSuiteVersion) -> s).toMap

    val rsRows = flowDataSets.combined.map(defaultCombinedFolder(stepIds))

    lazy val rsName = register_rule_suite_group_variable(rsRows)

    if (flowSteps.isEmpty) {
      throw FlowException(s"Flow $flowId could not be loaded as the Step's dataset does not contain that flow")
    }
    val thisFlow = flows.filter(flowFilter(flowId))
    val flowRow =
      if (thisFlow.isEmpty)
        FlowRow[FG](flowId.id, flowId.version, flowAuditDefault, defaultFlowDuration, None)
      else {
        val f = thisFlow.head()
        // register all the relevant groups
        f.flowRuleGroup.foreach{
          g =>
            fgP.load(stepIds, rsRows, g)
        }
        f
      }

    val thisSteps = sourceSteps.map{
        step =>
          val rid = Id(step.ruleSuiteId, step.ruleSuiteVersion)
          val rs = operation(flowId, rsName, rsRows, rid, flowRow, step)

          val views = viewRows.map{ s =>
            s.filter(s"ruleSuiteId = ${step.ruleSuiteId} and ruleSuiteVersion = ${step.ruleSuiteVersion}").collect().toSeq
          }.getOrElse(Seq.empty)
          val maps = mapRows.map{ s =>
            s.filter(s"ruleSuiteId = ${step.ruleSuiteId} and ruleSuiteVersion = ${step.ruleSuiteVersion}").collect().toSeq
          }.getOrElse(Seq.empty)

          Step(name = step.name, dependencies = step.dependencies, operation =
            Operation[RP#ResType](rs, step.operation.function, step.operation.resultApproach,
              step.operation.fieldName, step.operation.combineAuditWith),
            initConfiguration = StepInitConfiguration(views, maps),
            data = step.data, step.options)
      }

    FlowData[FG, RP#ResType](flowRow, thisSteps)
  }

  /**
   * Converts all Step level CombinedRuleSuiteRows into Ids by adding them to the [[FlowRuleGroup.ruleGroup]]
   * @param fullFlow
   * @return
   */
  def convertToIds(fullFlow: FullFlow[CombinedRuleSuiteRows, CombinedRuleSuiteRows]): FullFlow[CombinedRuleSuiteRows, Id] = {

    FullFlow(
      flowRow = {
        val group = fullFlow.flowRow.flowRuleGroup.map(_.ruleGroup).getOrElse(Seq.empty) ++ fullFlow.steps.map(_.ruleSuite)
        val name = fullFlow.flowRow.flowRuleGroup.map(_.ruleGroupName).getOrElse(defaultConvertGroupName)
        fullFlow.flowRow.copy( flowRuleGroup = Some( FlowRuleGroup(name, group) ) )
      },
      steps = fullFlow.steps.map {
        fullStep =>
          FullStep[Id](fullStep.step, Id(fullStep.ruleSuite.ruleSuiteId, fullStep.ruleSuite.ruleSuiteVersion), fullStep.initConfiguration)
      }
    )
  }

}