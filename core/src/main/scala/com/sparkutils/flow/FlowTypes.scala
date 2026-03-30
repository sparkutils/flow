package com.sparkutils.flow

import com.sparkutils.quality.RuleSuite
import com.sparkutils.quality.impl.views.ViewConfig

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
 * Attempts to merge fields within selectExpr("*", "fieldName.*").
 * As columns are used it may invoke scans, prefer using delta over parquet.
 */
@SerialVersionUID(1L)
case object MergeFields extends ResultApproach

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
 * @param options function specific options to process
 */
@SerialVersionUID(1L)
case class Operation(function: String, fieldName: String, options: Map[String, String], resultApproach: ResultApproach)

// TODO adding maps requires moving to spark 4

/**
 * Each step represents a ruleSuite applied via an operation over a view.  This resulting dataframe is then passed to a callback
 * @param ruleSuite
 * @param inputView
 * @param views
 * @param operation
 * @param properties properties associated with this step, for example filenames or id's to save results against
 * @param outputView the output view name
 * @param combineAuditWith when defined will trigger a group_results( unify_result ) with the operation fieldName
 * @param cacheResults should the resulting dataframe be cached
 */
@SerialVersionUID(1L)
case class Step(ruleSuite: RuleSuite, inputView: String, views: Seq[ViewConfig], operation: Operation,
                properties: Map[String, String], outputView: String, combineAuditWith: Option[String] = None, cacheResults: Boolean = false)
