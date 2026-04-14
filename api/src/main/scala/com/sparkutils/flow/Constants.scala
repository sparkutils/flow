package com.sparkutils.flow


import scala.concurrent.duration.{Duration, MINUTES}

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

}
