package com.sparkutils.flow

import com.sparkutils.quality.RuleSuiteParam
import org.apache.spark.sql.{Column, DataFrame}

/**
 * How should the result be used
 */
sealed trait ResultApproach extends Serializable

/**
 * Keeps the configured processor field as a nested type e.g. selectExpr("*"), this can be changed by a ResultProcessor
 */
@SerialVersionUID(1L)
case object AsIs extends ResultApproach

/**
 * Merges fields within selectExpr("*", "fieldName.result.*").
 *
 * This __keeps__ fields from the input dataset which are not present in the engine/folder result.
 *
 * As columns are used it may invoke scans, prefer using delta over parquet, or specify the startingStruct ddl in the [[Step.options]].
 */
@SerialVersionUID(1L)
case object MergeFields extends ResultApproach

/**
 * Equivalent to selectExpr("fieldName.result.*")
 *
 * This __discards__ fields from the input dataset, excluding flow_audit, which are not present in the engine/folder result.
 *
 * As columns are used it may invoke scans, prefer using delta over parquet, or specify the startingStruct ddl in the [[Step.options]].
 */
@SerialVersionUID(1L)
case object OutputFieldsOnly extends ResultApproach

/**
 * Treats the result as the new row by selectExpr("fieldName.*")
 */
@SerialVersionUID(1L)
case object StarOnly extends ResultApproach

/**
 * Treats the result as the new row by selectExpr("fieldName")
 */
@SerialVersionUID(1L)
case object OutputFieldOnly extends ResultApproach

trait CustomResultApproach extends Serializable {
  def apply[RP: RuleSuiteParam](input: DataFrame, function: Column, engineInputs: RunnerInputs,
            resultProcessInputs: ResultProcessInputs, step: Step[RP]): DataFrame
}

/**
 * Processes the result via a custom class implementing the CustomResultApproach trait
 */
@SerialVersionUID(1L)
case class CustomApproach(className: String) extends ResultApproach {
  @transient
  lazy val customResultApproach: CustomResultApproach = ClassUtils.load(this, className)
}

