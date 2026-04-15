package com.sparkutils.flow

import com.sparkutils.quality.{DataFrameLoader, MapRow, RuleSuite, ViewRow}
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
 * @param ruleSuite the rulesuite used by the named [[function]]
 * @param function either the quality function collect
 * @param fieldName the expression column name
 * @param resultApproach how should the resulting data be processed
 * @param combineAuditWith provide additional expressions to group_audit e.g. group_audit( operationFieldName, audit expression 1,
 *                         audit expression 2, ... )
 *                         with the operation fieldName automatically provided
 */
@SerialVersionUID(1L)
case class Operation(ruleSuite: RuleSuite, function: String, fieldName: String, resultApproach: ResultApproach,
                     combineAuditWith: Option[Set[String]] = None)
  extends Serializable

// TODO adding maps requires moving to spark 4


/**
 * Represents configuration information for a step, views and maps
 * @param viewConfig
 * @param mapConfig
 */
@SerialVersionUID(1L)
case class StepInitConfiguration(viewConfig: Seq[ViewRow] = Seq.empty, mapConfig: Seq[MapRow] = Seq.empty) extends Serializable

/**
 * Input and output data usage for a step
 * @param inputView the optional input view name, when provided it will be loaded, when not provided the output of
 *                  the dependency will be used.  It is an error to not specify an input name when there are more than
 *                  one dependency
 * @param outputView the output view name, when not specified the step name is used.
 * @param cacheResults should the resulting dataframe be cached
 */
@SerialVersionUID(1L)
case class StepData(inputView: Option[String] = None, outputView: Option[String] = None,
                    cacheResults: Boolean = false) extends Serializable

object StepData {
  def apply(inputView: String, outputView: String, cacheResults: Boolean): StepData =
    StepData(Option(inputView), Option(outputView), cacheResults)
  def apply(inputView: String, outputView: String): StepData =
    StepData(Option(inputView), Option(outputView))
}

/**
 * Each step represents a ruleSuite applied via an operation over a view.  This resulting dataframe is then passed to a
 * callback
 * @param name this steps name
 * @param dependencies the set of steps which must be run before this Step can start
 * @param operation the operation this step performs
 * @param initConfiguration configuration items used to prepare a step such as maps or views to be loaded
 * @param data the input and output views to be used by this Step.
 * @param options properties associated with this step, for example filenames or id's to save results against
 */
@SerialVersionUID(1L)
case class Step(name: String, dependencies: Set[String], operation: Operation,
                initConfiguration: StepInitConfiguration = StepInitConfiguration(),
                data: StepData = StepData(), options: Map[String, String] = Map.empty) extends Serializable {
  /**
   * The default output viewname for this step
   * @return
   */
  def defaultOutputViewName: String = data.outputView.getOrElse(name)
}
