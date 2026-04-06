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
 * @param properties properties associated with this step, for example filenames or id's to save results against
 * @param outputView the output view name
 * @param combineAuditWith provide additional expressions to group_audit e.g. group_audit( operationFieldName, audit expression 1,
 *                         audit expression 2, ... )
 *                         with the operation fieldName automatically provided
 * @param cacheResults should the resulting dataframe be cached
 */
@SerialVersionUID(1L)
case class Step(name: String, dependencies: Set[String], ruleSuite: RuleSuite, inputView: String, views: Seq[ViewRow], operation: Operation,
                properties: Map[String, String], outputView: String, combineAuditWith: Option[Set[String]] = None,
                cacheResults: Boolean = false) extends Serializable

/**
 * Trait used to handle extra control of data before and after processing a step.
 *
 * Derive and mix in with Flow creation to allow writing to catalog's or custom storage layers
 */
trait FlowDataHandling extends Serializable {

  def loader: DataFrameLoader

  /**
   * By default will attempt loading data by treating the token as a view/catalog item directly.  If this does not work
   * it will be forwarded to the Flow's loader
   *
   * @param sparkSession
   * @param loader
   * @param token
   * @return
   */
  protected def loadData(sparkSession: SparkSession, token: String): DataFrame =
    Try{sparkSession.sql(s"select * from `${token}` ")}.getOrElse{
      loader.load(token)
    }

  /**
   * Provides the struct type for a steps input data frame as used by startStep.
   *
   * If Step provides a configuration of inputSchema with DDL text this will be returned instead
   *
   * @param input
   * @param step
   * @return
   */
  protected def inputSchema(input: DataFrame, step: Step): StructType =
    step.properties.get("inputSchema").fold(
      input.schema
    ) { prop =>
      DataType.fromDDL(prop).asInstanceOf[StructType]
    }

  /**
   * By default, logs and returns input a dataframe using loadData with the token Step.inputViewName
   *
   * @param input either an empty dataset or the previous steps dataframe
   * @param step
   * @return the actual dataset used as input to the set
   */
  protected def startStep(input: DataFrame, step: Step): DataFrame

  /**
   * The default implementation optionally caches (cacheStepResults) and uses the outputViewName to register a temp view
   * @param step
   * @return a, by default, optionally cached dataFrame
   */
  protected def stepCompleted(result: DataFrame, step: Step): DataFrame

}