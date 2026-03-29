package com.sparkutils.flow

import com.sparkutils.quality.{DefaultProcessor, Id, NoOpDefaultProcessor, OutputExpression, VersionedId, collectRunner, expressionRunner, ruleEngineRunner, ruleFolderRunner, typedExpressionRunner}
import com.sparkutils.quality.impl.views.ViewLoadResults
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import Utils._
import org.apache.spark.sql.types.DataType

/**
 * Represents a number of steps for processing data
 *
 * @param steps steps which are processed in order
 * @param cacheStepResults each step result should be cached before providing the next step
 * @param showInterim calls show on interim results
 */
class Flow(ruleSuiteGroup: VersionedId, steps: Seq[Step], cacheStepResults: Boolean = false, showInterim: Boolean = false) {
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[Flow])
  import logger._

  // TODO move this to the server, allowing upgrades for all jobs on shared cluster
  private def process(dataFrame: DataFrame, step: Step, index: Int): DataFrame = {
    import step.operation._
    val col: Column =
      function.toLowerCase.replaceAll("_","") match {
        case "collect" | "collectrunner" =>
          collectRunner(step.ruleSuite, resultDataType = options.dataType("resultDataType"),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20),
            flatten = options.boolean("flatten", true),
            includeNulls = options.boolean("includeNulls"),
            useInPlaceArray = options.boolean("useInPlaceArray", true),
            unrollInPlaceArray = options.boolean("unrollInPlaceArray"),
            unrollOutputArraySize = options.int("unrollOutputArraySize", 1))
        case "engine" | "ruleengine" | "ruleenginerunner" =>
          ruleEngineRunner(step.ruleSuite,
            resultDataType = options.dataType("resultDataType"),
            debugMode = options.boolean("debugMode"),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20))
        case "folder" | "folderrunner" =>
          val rs =
            if (step.ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
              // even if the result is null, it's been chosen as such
              step.ruleSuite
            else
              // identity function - let the row through
              step.ruleSuite.copy(defaultProcessor = DefaultProcessor(Id(-1,-1), OutputExpression("row -> row")))

          ruleFolderRunner(rs,
            startingStruct = options.expr("startingStruct").getOrElse{
              // default to the current row
              expr("struct(*)")
            },
            useType = options.structType("useType").orElse(options.structType("resultDataType")),
            debugMode = options.boolean("debugMode"),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20))
        // TODO dq ?
      }
    val rdf = dataFrame.select(expr("*"), col.as(fieldName))
    resultApproach match {
      case AsIs => rdf
      case ExpandNested => rdf.selectExpr("*", s"$fieldName.*")
      case MergeFields => // dq probably doesn't work
        val og = rdf.columns.toSet
        val full = rdf.selectExpr("*", s"$fieldName.result.*", fieldName).columns
        val dupes = full.groupBy(identity).filter( p => p._2.length > 1 ).keys.toSet
        rdf.selectExpr(((og -- dupes).toSeq ++ Seq( s"$fieldName.result.*", s"$fieldName.ruleSuiteResults as $fieldName")) :_*)
      case StarOnly => rdf.selectExpr(s"$fieldName.*")
      case OutputFieldOnly => rdf.selectExpr(s"$fieldName")
    }
  }

  /**
   * Process the data starting with the provided DataFrame registered as the first steps inputView
   * @param sparkSession
   * @param starting
   * @return
   */
  def run(sparkSession: SparkSession, starting: DataFrame): DataFrame = {
    if (steps.nonEmpty) {
      starting.createOrReplaceTempView(steps.head.inputView)
      run(sparkSession)
    } else {
      throw FlowException("Empty Flow provided")
    }
  }

  /**
   * Process using the registered views
   * @param sparkSession
   * @param starting
   * @return
   */
  def run(sparkSession: SparkSession): DataFrame = {
    info(s"Starting Flow id: ${ruleSuiteGroup.id}, version: ${ruleSuiteGroup.version}")

    if (steps.nonEmpty) {
      val df = steps.zipWithIndex.foldLeft(sparkSession.sql(s"select * from `${steps.head.inputView}` ")) {
        case (df, (step, index)) =>
          val vl = com.sparkutils.quality.loadViews(step.views)
          stepViewsLoaded(vl, step, index)
          val starter = startStep(df, step, index)
          val res = process(starter, step, index)

          if (isDebugEnabled || showInterim) {
            infoLogStep(step, index, "Result Sample")
            res.show()
          }

          stepCompleted(res, step, index)
      }

      info(s"Finished Flow id: ${ruleSuiteGroup.id}, version: ${ruleSuiteGroup.version}")
      df
    } else {
      throw FlowException("Empty Flow provided")
    }
  }

  /**
   * By default, throws errors with failedToLoadDutToCycles or if the step inputView could not be loaded
   * @param vl
   * @param step
   * @param index
   */
  protected def stepViewsLoaded(vl: ViewLoadResults, step: Step, index: Int) = {
    if (vl.replaced.nonEmpty) {
      info(s"")
    }

    if (vl.failedToLoadDueToCycles && step.views.nonEmpty) {
      val err = FlowException(s"View Cycle - Step $index, RuleSuite id: ${ruleSuiteGroup.id}, version: ${ruleSuiteGroup.version} could not be started due to view cycle detection")
      error(err.msg)
      throw err
    }
    if (vl.notLoadedViews.nonEmpty) {
      if (vl.notLoadedViews.contains(step.inputView)) {
        val err = FlowException(s"View Cycle - Step $index, RuleSuite id: ${ruleSuiteGroup.id}, version: ${ruleSuiteGroup.version} could not be started as the Steps inputView `${step.inputView}` could not be loaded")
        error(err.msg)
        throw err
      } else {
        val msg = s"View Cycle - Step $index, RuleSuite id: ${ruleSuiteGroup.id}, version: ${ruleSuiteGroup.version} could not load the following views - "
        warn(msg + vl.notLoadedViews.map(v => s"`$v`").mkString)
      }
    } else {
      infoLogStep(step, index, "Views Loaded")
    }
  }

  /**
   * Logs at info leve the step
   * @param step
   * @param index
   * @param logInfo
   */
  final protected def infoLogStep(step: Step, index: Int, logInfo: String): Unit = {
    info(s"Step $index, RuleSuite id: ${ruleSuiteGroup.id}, version: ${ruleSuiteGroup.version} $logInfo")
  }

  /**
   * By default, logs and returns input a dataframe from Step.inputViewName
   *
   * @param input either an empty dataset or the previous steps dataframe
   * @param step
   * @param index
   * @return the actual dataset used as input to the set
   */
  protected def startStep(input: DataFrame, step: Step, index: Int): DataFrame = {
    infoLogStep(step, index, "Started")
    input.sparkSession.sql(s"select * from `${step.inputView}`")
  }

  /**
   * The default implementation optionally caches (cacheStepResults) and, where present, uses the view name of the next step
   * @param step
   * @return a, by default, optionally cached dataFrame
   */
  protected def stepCompleted(result: DataFrame, step: Step, index: Int): DataFrame = {
    val ndf =
      if (cacheStepResults)
        result.cache
      else
        result

    ndf.createOrReplaceTempView(step.outputView)

    infoLogStep(step, index, "Completed")

    ndf
  }

}