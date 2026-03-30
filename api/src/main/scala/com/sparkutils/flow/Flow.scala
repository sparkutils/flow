package com.sparkutils.flow

import com.sparkutils.quality.{DefaultProcessor, Id, NoOpDefaultProcessor, OutputExpression, VersionedId, collectRunner, expressionRunner, ruleEngineRunner, ruleFolderRunner, typedExpressionRunner}
import com.sparkutils.quality.impl.views.ViewLoadResults
import org.apache.spark.sql.functions.{array, expr, col => scol}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import Utils._
import com.sparkutils.quality.functions.group_results
import org.apache.spark.sql.types.DataType

/**
 * Represents a number of steps for processing data
 *
 * @param steps steps which are processed in order
 * @param showInterim calls show on interim results
 *
 */
@SerialVersionUID(1L)
class Flow(ruleSuiteGroup: VersionedId, steps: Seq[Step], showInterim: Boolean = false, flowAuditColName: String = "flow_audit", inline: Boolean = false) extends Serializable {
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[Flow])
  import logger._

  // TODO move this to the server, allowing upgrades for all jobs on shared cluster
  private def process(dataFrame: DataFrame, step: Step, index: Int): DataFrame = {
    import step.operation._
    val (col: Column, childrenRaw: Seq[String]) = (
      function.toLowerCase.replaceAll("_","") match {
        case "collect" | "collectrunner" =>
          (collectRunner(step.ruleSuite, resultDataType = options.dataType("resultDataType"),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20),
            flatten = options.boolean("flatten", true),
            includeNulls = options.boolean("includeNulls"),
            useInPlaceArray = options.boolean("useInPlaceArray", true),
            unrollInPlaceArray = options.boolean("unrollInPlaceArray"),
            unrollOutputArraySize = options.int("unrollOutputArraySize", 1)).as(fieldName), Seq("ruleSuiteResults", "result"))
        case "engine" | "ruleengine" | "ruleenginerunner" =>
          (ruleEngineRunner(step.ruleSuite,
            resultDataType = options.dataType("resultDataType"),
            debugMode = options.boolean("debugMode"),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20)).as(fieldName), Seq("ruleSuiteResults", "salientRule", "result"))
        case "folder" | "folderrunner" =>
          val rs =
            if (step.ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
              // even if the result is null, it's been chosen as such
              step.ruleSuite
            else
              // identity function - let the row through
              step.ruleSuite.copy(defaultProcessor = DefaultProcessor(Id(-1,-1), OutputExpression("row -> row")))

          (ruleFolderRunner(rs,
            startingStruct = options.expr("startingStruct").getOrElse{
              // default to the current row
              expr("struct(*)")
            },
            useType = options.structType("useType").orElse(options.structType("resultDataType")),
            debugMode = options.boolean("debugMode"),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20)).as(fieldName), Seq("ruleSuiteResults", "result"))
        // TODO dq ?
      } )

    val children = childrenRaw.map(col.getField)

    def wrapped() = {
      val rdf = dataFrame.select(expr("*"), col.as(fieldName))
      val notUnified =
        resultApproach match {
          case AsIs => rdf
          case ExpandNested => rdf.selectExpr("*", s"$fieldName.*")
          case MergeFields => // dq probably doesn't work
            val og = rdf.columns.toSet
            val full = rdf.selectExpr("*", s"$fieldName.result.*", fieldName).columns
            val dupes = full.groupBy(identity).filter(p => p._2.length > 1).keys.toSet
            rdf.selectExpr(((og -- dupes).toSeq ++ Seq(s"$fieldName.result.*", fieldName)): _*)
          case StarOnly => rdf.selectExpr(s"$fieldName.*")
          case OutputFieldOnly => rdf.selectExpr(s"$fieldName")
        }

      step.combineAuditWith.map { fname =>
        notUnified.withColumn(flowAuditColName,
          expr(s"group_results( array( $fieldName.ruleSuiteResults, $fname.ruleSuiteResults ) )"))
      }.getOrElse(notUnified)
    }

    def inlineImpl() = {
      val columns = Seq(expr("*"), col) ++
        step.combineAuditWith.map{ fname =>
          Seq(group_results( array( col.getField("ruleSuiteResults"), scol(fname).getField("ruleSuiteResults") ) ).
            as(flowAuditColName))
        }.getOrElse(Seq.empty)

      //val rdf = dataFrame.select(Seq(expr("*"), col.as(fieldName)) ++ statsColumn :_*)
      resultApproach match {
        case AsIs => dataFrame.select(columns :_*)
        case ExpandNested => dataFrame.select(columns ++ children :_*)//rdf.selectExpr("*", s"$fieldName.*")
        case MergeFields => // dq probably doesn't work

          val starter = dataFrame.select(columns : _*)
          val og = starter.columns.toSet + fieldName
          val nested = starter.selectExpr(s"$fieldName.result.*").columns
          starter.selectExpr((og -- nested).toSeq ++ Seq( s"$fieldName.result.*") :_*)

        case StarOnly => dataFrame.select(children: _*)
        case OutputFieldOnly => dataFrame.select(col)
      }
    }

    if (inline)
      inlineImpl()
    else
      wrapped()
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
      if (step.cacheResults)
        result.cache
      else
        result

    ndf.createOrReplaceTempView(step.outputView)

    infoLogStep(step, index, "Completed")

    ndf
  }

}