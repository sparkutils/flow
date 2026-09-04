package com.sparkutils.flow

import com.sparkutils.quality.{GroupRuleId, Id, MapRow, RuleSuite, ViewRow}
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration

trait FlowExceptionType { selfType: Exception =>
  def msg: String
  def cause: Throwable
}

@SerialVersionUID(1L)
case class FlowException(msg: String, cause: Throwable = null) extends Exception(msg, cause) with Serializable with FlowExceptionType

@SerialVersionUID(1L)
case class StepException[RP](step: Step[RP], cause: Throwable = null, timedOut: Boolean = false) extends Exception(cause) with Serializable with
  StepResultType[RP] with FlowExceptionType {

  override def fold[R](success: StepResult[RP] => R)(failure: StepException[RP] => R): R = failure(this)

  override def output: DataFrame = throw FlowException(s"Step ${step.name} did not complete successfully", cause)

  override def timings: StepTimings = throw FlowException(s"Step ${step.name} did not complete successfully", cause)

  def msg: String = cause.getMessage
}

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
case class Operation[T: RuleSuiteTypeParam](ruleSuite: T, function: Runner, resultApproach: ResultApproach,
                     fieldName: Option[String] = None,
                     combineAuditWith: Option[Set[String]] = None) extends Serializable

object Operation {

  def apply[T: RuleSuiteTypeParam](ruleSuite: T, function: Runner, resultApproach: ResultApproach,
            fieldName: String): Operation[T] = Operation(ruleSuite, function, resultApproach, Option(fieldName))
  // $COVERAGE-OFF$
  def apply[T: RuleSuiteTypeParam](ruleSuite: T, function: Runner, resultApproach: ResultApproach,
            fieldName: String, combineAuditWith: Set[String]): Operation[T] =
    Operation(ruleSuite, function, resultApproach, Option(fieldName), Option(combineAuditWith))
  // $COVERAGE-ON$

  def apply[T: RuleSuiteTypeParam](ruleSuite: T, function: Runner, resultApproach: ResultApproach,
                               combineAuditWith: Set[String]): Operation[T] =
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
    // $COVERAGE-OFF$
    require(mapName.isDefined, "mapName is required when MapConfig is provided")
    // $COVERAGE-ON$
  }
}

/**
 * Input and output data usage for a step
 * @param inputView the optional input view name, when provided it will be loaded, when not provided the output of
 *                  the dependency will be used.  It is an error to not specify an input name when there are more than
 *                  one dependency
 * @param outputView the output view name, when not specified the step name is used.
 * @param cacheResults should the resulting dataframe be cached
 * @param persistor the fully qualified name of the persistor to use, by default it performs no saves
 * @param persistorOptions the set of options provided to the persistor, this may include format and location,
 *                         or security tokens etc.  The entire set of options will be passed to the
 *                         DataFrameWriter where possible.
 */
@SerialVersionUID(1L)
case class StepData(inputView: Option[String] = None, outputView: Option[String] = None,
                    cacheResults: Boolean = false,
                    persistor: String = classOf[NoOpPersistor].getName,
                    persistorOptions: Map[String, String] = Map.empty) extends Serializable

object StepData {
  def apply(inputView: String, outputView: String, cacheResults: Boolean): StepData =
    StepData(Option(inputView), Option(outputView), cacheResults)
  def apply(inputView: String, outputView: String): StepData =
    StepData(Option(inputView), Option(outputView))
}

/**
 * Which types are allowed in an encoded Operation, similar to [[com.sparkutils.quality.RuleSuiteParam]] but no Column
 * instance exists.
 */
sealed trait RuleSuiteTypeParam[T] {
  def id(t: T): Id
}

object RuleSuiteTypeParam {

  implicit val rsParam: RuleSuiteTypeParam[RuleSuite] = new RuleSuiteTypeParam[RuleSuite] {
    override def id(t: RuleSuite): Id = t.id
  }

  implicit val idNameParam: RuleSuiteTypeParam[GroupRuleId] = new RuleSuiteTypeParam[GroupRuleId] {
    override def id(t: GroupRuleId): Id = t.id
  }

}

trait StepLike extends Serializable {
  def function: Runner
  def options: Map[String, String]
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
case class Step[T: RuleSuiteTypeParam](name: String, dependencies: Set[String], operation: Operation[T],
                initConfiguration: StepInitConfiguration = StepInitConfiguration(),
                data: StepData = StepData(), options: Map[String, String] = Map.empty) extends StepLike {
  /**
   * The default output viewname for this step
   * @return
   */
  def defaultOutputViewName: String = data.outputView.getOrElse(name)

  /**
   * The default field name or provided one, when present
   */
  def defaultFieldName: String = operation.fieldName.getOrElse(name)

  def function: Runner = operation.function

}

/**
 * Timings associated with Step processing
 * @param runner the preparation time take by the runner processing
 * @param result the preparation time take by the result processing
 */
@SerialVersionUID(1L)
case class StepTimings(runner: Duration, result: Duration) extends Serializable

sealed trait StepResultType[T] {
  def fold[R](success: StepResult[T] => R)(failure: StepException[T] => R): R
  def step: Step[T]
  def output: DataFrame
  def timings: StepTimings
}

/**
 * The result of a step
 * @param step this may be a modified Step if modifyStep was overridden
 * @param output the resulting DataFrame from this step
 * @param timings the time taken to prepare the output
 */
@SerialVersionUID(1L)
case class StepResult[RP: RuleSuiteTypeParam](step: Step[RP], output: DataFrame, timings: StepTimings) extends
  Serializable with StepResultType[RP] {

  override def fold[R](success: StepResult[RP] => R)(failure: StepException[RP] => R): R = success(this)

}

/**
 * The result of a flow, failure [[StepException]]s will be kept when using [[Flow.run]] with the tolerant parameter set to true
 * @param stepResults the original Step name mapped to it's result
 * @param duration the time taken by the entire flow
 */
@SerialVersionUID(1L)
case class FlowResult[T: RuleSuiteTypeParam](stepResults: Map[String, StepResultType[T]],
                                             duration: Duration) extends Serializable

