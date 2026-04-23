package com.sparkutils.flow

import org.apache.spark.sql.{Column, DataFrame}

/**
 * Runners that can be used by Flow Step operations
 */
sealed trait Runner extends Serializable

/**
 * Use the Quality ruleRunner for DQ
 */
@SerialVersionUID(1L)
case object DQ extends Runner

/**
 * Use the Quality collectRunner
 */
@SerialVersionUID(1L)
case object Collect extends Runner

/**
 * Use the Quality ruleEngine
 */
@SerialVersionUID(1L)
case object Engine extends Runner

/**
 * Use the Quality folder
 */
@SerialVersionUID(1L)
case object Folder extends Runner

/**
 * Custom implementation of runners
 */
trait CustomRunner extends Serializable {
  /**
   * Called if a [[Step.operation]] is configured with a CustomRunnerEngine.
   *
   * Custom implementations could include saving data or performing service lookups
   *
   * @param dataFrame provided for convenience, but should not be required
   * @param engineInputs key input attributes needed to create the output
   * @param step
   * @return
   */
  def apply(dataFrame: DataFrame, engineInputs: EngineInputs, step: Step): RunnerOutput
}

/**
 * Processes the result via a custom class implementing the CustomResultApproach trait
 */
@SerialVersionUID(1L)
case class CustomRunnerEngine(className: String) extends Runner {
  @transient
  lazy val customRunner: CustomRunner = ClassUtils.load(this, className)
}
