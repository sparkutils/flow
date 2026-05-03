package com.sparkutils.flow


import com.sparkutils.quality.GroupRuleId

import scala.concurrent.duration.{Duration, FiniteDuration, HOURS, MINUTES}

trait Constants {

  /**
   * Optionally specify this [[Step.options]] in all three Quality runners to control the output type
   */
  val resultDataType = "resultDataType"
  /**
   * The default name used by Flow for the flow's auto-audit column
   */
  val flowAuditDefault = "flow_audit"

  /**
   * The collect runner name for [[Operation.function]]
   * @group collect runner
   */
  val CollectRunnerName = "collect"
  /**
   * Optional collect runner [[Step.options]] to flatten nested arrays, by default true
   * @group collect runner
   */
  val collectFlatten = "flatten"
  /**
   * Optional collect runner [[Step.options]] to includeNulls in the resulting array, by default false
   * @group collect runner
   */
  val collectIncludeNulls = "includeNulls"
  /**
   * Optional collect runner [[Step.options]] to use the InPlaceArray optimisation, by default true.
   * @group collect runner
   * @group advanced property
   */
  val collectUseInPlaceArray = "useInPlaceArray"
  /**
   * Optional collect runner [[Step.options]] to unroll loops with the InPlaceArray optimisation, ba default false.
   *
   * This should only be used when OutputExpressions return arrays with large numbers of entries, the JVM JIT is otherwise
   * faster.
   *
   * @group collect runner
   * @group advanced property
   */
  val collectUnrollInPlaceArray = "unrollInPlaceArray"
  /**
   * Optional collect runner [[Step.options]] to control the size of batching in the loop unrolling optimisation,
   * by default this is not used, but has a value of 1, implying all entries should be unrolled.
   *
   * This will be slower for very large array sizes from the OutputExpressions, and increasing the number can improve
   * performance by nesting loops.
   *
   * @group collect runner
   * @group advanced property
   */
  val collectUnrollOutputArraySize = "unrollOutputArraySize"

  /**
   * The engine runner name for [[Operation.function]]
   * @group engine runner
   */
  val EngineRunnerName = "engine"

  /**
   * The engine runner name for [[Operation.function]]
   * @group folder runner
   */
  val FolderRunnerName = "folder"

  /**
   * Optional engine and folder runner [[Step.options]] to use debug mode output
   * @group engine runner
   * @group collect runner
   * @group advanced property
   */
  val debugMode = "debugMode"

  /**
   * Optional folder runner [[Step.options]] to control the starting struct expression, by default this will use the
   * incoming schema, which requires analysis of the dataframe and, with parquet, can lead to full file system scans,
   * known to be very slow on azure, for example.
   *
   * Prefer to specify this when the cost of obtaining the schema is too high (you can use the
   * [[inputSchemaTimeTakenWarning]] configuration to trigger a warning log entry to observe this, by default it's 1 minute).
   *
   * @group folder runner
   */
  val startingStruct = "startingStruct"


  /**
   * Optional [[Step.options]] to force the use of an additional projection, by default false.  Used by [[MergeFields]]
   * and [[OutputFieldsOnly]] [[Operation.resultApproach]]s
   */
  val forceMergeProjection = "forceMergeProjection"

  /**
   * Optional [[Step.options]] to specify the input schema to use for a Step should the cost of deriving the schema
   * be too high.
   *
   * @group input schema
   */
  val inputSchema = "inputSchema"
  /**
   * Optional [[Step.options]] to specify [[scala.concurrent.duration.Duration]] compatible string for how long a
   * [[org.apache.spark.sql.Dataset.schema]] call should take before a warning is logged.  e.g. "3minutes"
   *
   * @group input schema
   */
  val inputSchemaTimeTakenWarning = "inputSchemaTimeTakenWarning"
  /**
   * The default value of 1minute used by [[inputSchemaTimeTakenWarning]]
   *
   * @group input schema
   */
  val inputSchemaTimeTakenDefault: Duration = Duration(1, MINUTES)

  /**
   * DQ Runner name
   */
  val DQRunnerName = "dq"

  /**
   * DQ Runner name
   */
  val NoOpRunnerName = "noop"

  /**
   * The column name which must be returned by [[flowEarlyExitSQL]] and evaluate to either an empty dataset
   * or to a *true* boolean value on the first row to indicate the flow should continue.
   * Otherwise, if the first row with a *false* value is returned the Flow will exit with exception per
   * [[flowEarlyExitException]]
   */
  val flowEarlyExitColumn = "flowEarlyExitColumn"

  /**
   * Optional [[Step.options]] to specify the Name for a step to run an SQL to decide if the flow should be exited.
   *
   * The SQL is evaluated after the Steps result view is created, by default the [[Step.name]].
   *
   * When present the SQL will be evaluated after a step is completed, it should return [[flowEarlyExitColumn]]
   * with it's associated semantics.
   *
   * NOTE - The graph runs in parallel, if any Steps must perform "safe" actions such as saving a data frame these checks
   * are recommended to be done as dependencies of those Steps
   */
  val flowEarlyExitSQL = "flowEarlyExitSQL"

  /**
   * Optional [[Step.options]] to specify a user Exception text if [[flowEarlyExitSQL]] returns a false
   * [[flowEarlyExitColumn]] result.
   *
   * When not present a default text is used:
   * {{{
   * Step ${step.name} had flowEarlyExitSQL configured which returned false - the Flow will be stopped
   * }}}
   */
  val flowEarlyExitException = "flowEarlyExitException"

  /**
   * The suffix used for root Steps if, as is default, no inputViewName is defined
   */
  val rootInputSuffix = "_root_input"

  /**
   * By default, flows do not have time limits imposed
   */
  val defaultFlowDuration: Duration = Duration.Inf

  /**
   * This optional [[Step.options]] configuration, defaulting to true, takes effect when the [[Step]]'s
   * [[Operation.ruleSuite]] type is [[com.sparkutils.quality.Id]] and [[FlowRuleGroup.ruleGroup]] type is
   * [[com.sparkutils.quality.CombinedRuleSuiteRows]].  In this case the [[FlowRuleGroup.ruleGroupName]]
   * is used and data is taken from [[FlowRuleGroup.ruleGroup]].
   *
   * This setting on a Step forces a filter on the provided RuleSuite dataset to be used for the RuleSuite Id and uses
   * this name to register it.
   */
  val useFlowRuleGroupLevelRows = "useFlowRuleGroupLevelRows"

  /**
   * This optional [[Step.options]] configuration, takes effect when the [[Step]]'s
   * [[Operation.ruleSuite]] type is [[com.sparkutils.quality.Id]].  It overrides whatever logic is used by
   * [[OperationProcessing]] with Id including [[useFlowRuleGroupLevelRows]]
   */
  val useRuleGroupName = "useRuleGroupName"

  /**
   * This optional [[Step.options]] configuration, defaulting to false, stops modifying [[Operation.ruleSuite]]'s
   * defaultProcessor when [[Operation.function]] is [[FolderRunnerName]] to introduce a default {{row -> row}}.
   */
  val doNotAddFolderDefault = "doNotAddFolderDefault"

  /**
   * The defaultGroup value is used when calling [[convertToIds]] should there be no flowRuleGroup present
   */
  val defaultConvertGroupName = "defaultGroup"

  /**
   * When added to a [[Step.options]] configuration it specifies how long a Step will be allowed to process before
   * a timeout is triggered, this will be signaled in [[StepException.timedOut]].  The timeout does not stop the underlying
   * Step, but forces the dependent graph below it to fail.
   *
   * The failure is either a complete exit, tolerant = false, or the [[StepException.timedOut]] will be true for the
   * source Step.
   */
  val stepTimeoutName = "stepTimeout"
}
