package com.sparkutils.flow

import com.sparkutils.quality.{DataFrameLoader, RuleSuite, ViewRow}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

@SerialVersionUID(1L)
case class FlowException(msg: String, cause: Throwable = null) extends Exception(msg, cause) with Serializable

/**
 * How should the result be used
 */
sealed trait ResultApproach extends Serializable
// todo should the interim being kept be result
/**
 * Keeps the configured processor field as a nested type e.g. selectExpr("*"), this can be changed by a ResultProcessor
 */
@SerialVersionUID(1L)
case object AsIs extends ResultApproach

/**
 * Expands nested result field via selectExpr("*", "fieldName.*"), but does not manage duplicates
 */
@SerialVersionUID(1L)
case object ExpandNested extends ResultApproach

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

/**
 * Represents an operation on a dataset
 * @param function either the quality function collect
 * @param fieldName the expression column name
 */
@SerialVersionUID(1L)
case class Operation(function: String, fieldName: String, resultApproach: ResultApproach)
  extends Serializable

// TODO adding maps requires moving to spark 4

/**
 * Each step represents a ruleSuite applied via an operation over a view.  This resulting dataframe is then passed to a
 * callback
 * @param name this steps name
 * @param dependencies the set of steps which must be run before this Step can start
 * @param ruleSuite
 * @param inputView
 * @param views
 * @param operation
 * @param options properties associated with this step, for example filenames or id's to save results against
 * @param outputView the output view name
 * @param combineAuditWith provide additional expressions to group_audit e.g. group_audit( operationFieldName, audit expression 1,
 *                         audit expression 2, ... )
 *                         with the operation fieldName automatically provided
 * @param cacheResults should the resulting dataframe be cached
 */
@SerialVersionUID(1L)
case class Step(name: String, dependencies: Set[String], ruleSuite: RuleSuite, inputView: String, views: Seq[ViewRow], operation: Operation,
                options: Map[String, String], outputView: String, combineAuditWith: Option[Set[String]] = None,
                cacheResults: Boolean = false) extends Serializable
