package com.sparkutils.flow

import com.sparkutils.quality.impl.Encoders
import com.sparkutils.quality.{DataFrameLoader, MapRow, RuleSuite, ViewRow}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.util.Try

@SerialVersionUID(1L)
case class FlowException(msg: String, cause: Throwable = null) extends Exception(msg, cause) with Serializable

/**
 * Represents an operation on a dataset
 * @param ruleSuite the rulesuite used by the named [[function]]
 * @param function either the quality function collect
 * @param fieldName the optional expression column name, when not provided the [[Step.defaultFieldName]] is used
 * @param resultApproach how should the resulting data be processed
 * @param combineAuditWith provide additional expressions to group_audit e.g. group_audit( operationFieldName, audit expression 1,
 *                         audit expression 2, ... )
 *                         with the operation fieldName automatically provided
 */
@SerialVersionUID(1L)
case class Operation(ruleSuite: RuleSuite, function: Runner, resultApproach: ResultApproach,
                     fieldName: Option[String] = None,
                     combineAuditWith: Option[Set[String]] = None) extends Serializable

object Operation {

  def apply(ruleSuite: RuleSuite, function: Runner, resultApproach: ResultApproach,
            fieldName: String): Operation = Operation(ruleSuite, function, resultApproach, Option(fieldName))

  def apply(ruleSuite: RuleSuite, function: Runner, resultApproach: ResultApproach,
            fieldName: String, combineAuditWith: Set[String]): Operation =
    Operation(ruleSuite, function, resultApproach, Option(fieldName), Option(combineAuditWith))

  def apply(ruleSuite: RuleSuite, function: Runner, resultApproach: ResultApproach, combineAuditWith: Set[String]): Operation =
    new Operation(ruleSuite, function, resultApproach, combineAuditWith = Option(combineAuditWith))

}

/**
 * Represents configuration information for a step, views and maps.  If a map is defined a mapName must also be provided
 * @param viewConfig
 * @param mapConfig
 * @param mapName
 */
@SerialVersionUID(1L)
case class StepInitConfiguration(viewConfig: Seq[ViewRow] = Seq.empty, mapConfig: Seq[MapRow] = Seq.empty,
                                 mapName: Option[String] = None) extends Serializable {
  if (mapConfig.nonEmpty) {
    require(mapName.isDefined, "mapName is required when MapConfig is provided")
  }
}

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

  /**
   * The default field name or provided one, when present
   */
  def defaultFieldName: String = operation.fieldName.getOrElse(name)

}

@SerialVersionUID(1L)
case class StepResult(step: Step, output: DataFrame) extends Serializable