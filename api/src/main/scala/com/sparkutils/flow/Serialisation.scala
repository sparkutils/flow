package com.sparkutils.flow

import com.sparkutils.quality
import com.sparkutils.quality.implicits.combinedRuleSuiteRowTypedExpEnc
import com.sparkutils.quality.{Id, LambdaFunctionRow, MapRow, OutputExpressionRow, RuleRow, RuleSuiteRow, VersionedId, ViewRow, register_rule_suite_group_variable, rule_suite, rule_suite_from, toDS, toLambdaDS, toOutputExpressionDS, toRuleSuiteRow}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions.col

@SerialVersionUID(1L)
case class OperationRow(function: String, fieldName: Option[String], resultApproach: ResultApproach, combineAuditWith: Option[Set[String]] = None)

@SerialVersionUID(1L)
case class StepRow(flowId: Int, flowVersion: Int, name: String, dependencies: scala.collection.immutable.Set[String], ruleSuiteId: Int,
                   ruleSuiteVersion: Int,
                   operation: OperationRow, options: Map[String, String], data: StepData)

@SerialVersionUID(1L)
case class FlowRow(flowId: Int, flowVersion: Int, flowAuditColName: String)

case class FlowDataSets(steps: Dataset[StepRow], flows: Dataset[FlowRow], ruleRows: Dataset[RuleRow],
                        lambdaFunctionRows: Option[Dataset[LambdaFunctionRow]] = None,
                        outputExpressionRows: Option[Dataset[OutputExpressionRow]] = None,
                        globalLambdaSuites: Option[Dataset[Id]] = None,
                        globalOutputExpressionSuites: Option[Dataset[Id]] = None,
                        ruleSuites: Option[Dataset[RuleSuiteRow]] = None,
                        viewRows: Option[Dataset[ViewRow]] = None,
                        mapRows: Option[Dataset[MapRow]] = None,
                       )

trait Serialisation {

  def loadSteps(dataFrame: DataFrame, flowId: Column, flowVersion: Column, name: Column, dependencies: Column,
                ruleSuiteId: Column, ruleSuiteVersion: Column, operation: Column,
                options: Column, data: Column): Dataset[StepRow] = {
    import frameless._
    import implicits._
    import com.sparkutils.quality.implicits._

    implicit val typed = TypedExpressionEncoder[StepRow]

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
   * Converts a Flow to it's underlying datasets, global output and lambdas are not supported
   * (they are treated as part of the ds directly)
   */
  def toDatasets(sparkSession: SparkSession, flow: Flow): FlowDataSets = {

    import frameless._
    import implicits._

    import com.sparkutils.quality.implicits._

    import sparkSession.implicits._

    implicit val typedStep = TypedExpressionEncoder[StepRow]
    implicit val typedFlow = TypedExpressionEncoder[FlowRow]

    val suites = flow.steps.map(s => toRuleSuiteRow(s.operation.ruleSuite))
    val defaultOutputRows = suites.flatMap(_._2)

    FlowDataSets(
      flow.steps.zipWithIndex.map{case (step, index) =>
        StepRow(flowId = flow.flowId.id, flowVersion = flow.flowId.version, name = step.name,
          dependencies = step.dependencies,
          ruleSuiteId = step.operation.ruleSuite.id.id, ruleSuiteVersion = step.operation.ruleSuite.id.version,
          data = step.data,
          operation = OperationRow(step.operation.function, step.operation.fieldName,
            step.operation.resultApproach, step.operation.combineAuditWith),
          options = step.options)}.toDS(),
      Seq(FlowRow(flowId = flow.flowId.id, flowVersion = flow.flowId.version,
        flowAuditColName = flow.flowAuditColName)).toDS(),
      flow.steps.foldLeft(sparkSession.emptyDataset[RuleRow]){ case (cur, s) =>
        toDS(s.operation.ruleSuite) union cur
      },
      Some(flow.steps.foldLeft(sparkSession.emptyDataset[LambdaFunctionRow]){ case (cur, s) =>
        toLambdaDS(s.operation.ruleSuite) union cur
      }),
      Some(flow.steps.foldLeft(sparkSession.emptyDataset[OutputExpressionRow]){ case (cur, s) =>
        toOutputExpressionDS(s.operation.ruleSuite) union cur
      } union defaultOutputRows.toDS()),
      ruleSuites = Some(suites.map(_._1).toDS()),
      viewRows = Some(flow.steps.foldLeft(sparkSession.emptyDataset[ViewRow]){ case (cur, s) =>
         s.initConfiguration.viewConfig.toDS() union cur
      }),
      mapRows = Some(flow.steps.foldLeft(sparkSession.emptyDataset[MapRow]){ case (cur, s) =>
        s.initConfiguration.mapConfig.toDS() union cur
      })
    )
  }

  def fromDatasets(sparkSession: SparkSession, flowDataSets: FlowDataSets, flowId: VersionedId): (Seq[Step], String) = {
    import flowDataSets._

    // TODO - if this cannot be just 4.x then the og serialisation functions need to be used

    import com.sparkutils.quality.implicits._

    val rsRows =
      quality.combine(ruleRows, lambdaFunctionRows, outputExpressionRows, globalLambdaSuites,
        globalOutputExpressionSuites, ruleSuites)

    val flowFilter = s"flowID = ${flowId.id} and flowVersion = ${flowId.version} "

    val flowSteps = loadSteps(steps.toDF()).filter(flowFilter)
    if (flowSteps.isEmpty) {
      throw FlowException(s"Flow $flowId could not be loaded as the Step's dataset does not contain that flow")
    }
    val thisFlow = flows.filter(flowFilter)
    val flowAuditColumn =
      if (thisFlow.isEmpty) {
        "flow_audit"
      } else {
        thisFlow.head().flowAuditColName
      }

    val thisSteps =
      flowSteps.collect().map{
        step =>
          val rid = Id(step.ruleSuiteId, step.ruleSuiteVersion)
          val rs = rule_suite(rsRows, rid).getOrElse(
            throw FlowException(s"Flow $flowId could not be loaded as a Step's RuleSuite $rid is missing")
          )
          val views = viewRows.map{ s =>
            s.filter(s"ruleSuiteId = ${step.ruleSuiteId} and ruleSuiteVersion = ${step.ruleSuiteVersion}").collect().toSeq
          }.getOrElse(Seq.empty)
          val maps = mapRows.map{ s =>
            s.filter(s"ruleSuiteId = ${step.ruleSuiteId} and ruleSuiteVersion = ${step.ruleSuiteVersion}").collect().toSeq
          }.getOrElse(Seq.empty)

          Step(name = step.name, dependencies = step.dependencies, operation =
            Operation(rs, step.operation.function, step.operation.resultApproach,
              step.operation.fieldName, step.operation.combineAuditWith),
            initConfiguration = StepInitConfiguration(views, maps),
            data = step.data, step.options)
      }

    (thisSteps, flowAuditColumn)
  }
}