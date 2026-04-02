package com.sparkutils.flow

import com.sparkutils.quality.{DataFrameLoader, DefaultProcessor, Id, NoOpDefaultProcessor, OutputExpression, VersionedId, collectRunner, expressionRunner, ruleEngineRunner, ruleFolderRunner, typedExpressionRunner}
import com.sparkutils.quality.impl.views.ViewLoadResults
import org.apache.spark.sql.functions.{array, expr, col => scol}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import Utils._
import com.sparkutils.quality.functions.group_results
import org.apache.spark.sql.types.DataType
import scalax.collection.ToString.{SetElemsOnSeparateLines, SetsOnSeparateLines}
import scalax.collection.edges.{DiEdge, DiEdgeImplicits, UnDiEdge, UnDiEdgeImplicits}
import scalax.collection.immutable.Graph

import scala.annotation.tailrec

/**
 * Represents a number of steps for processing data.  By default, views configured by token will throw not implemented,
 * provide a DataFrameLoader where tokens are used.
 *
 * @param steps steps which are processed in order
 * @param showInterim calls show on interim results
 * @param
 *
 */
@SerialVersionUID(1L)
class Flow(val flowId: VersionedId, val steps: Seq[Step],
           val flowAuditColName: String = "flow_audit", loader: DataFrameLoader = new DataFrameLoader {
              override def load(token: String): DataFrame = ???
            },
           showInterim: Boolean = false, viewColumns: ViewConfigColumns = ViewConfigColumns()) extends Serializable {
  private val logger = org.slf4j.LoggerFactory.getLogger(classOf[Flow])
  import logger._

  @transient
  lazy val theGraph: Graph[Step, DiEdge[Step]] = {
    val map = steps.map(s => s.name -> s).toMap

    val graph = steps.foldLeft(scalax.collection.immutable.Graph.from[Step, DiEdge[Step]](steps, Seq.empty)){
      (cur, step) =>
        if (step.dependencies.isEmpty)
          cur
        else {
          step.dependencies.foldLeft(cur){
            (cur, dependency) =>
              map.get(dependency).fold(
                throw FlowException(s"Step ${step.name} refers to a dependency $dependency Step which does not exist")
              ) { dependent =>
                cur + dependent ~> step
              }
          }
        }
    }

    //println(graph.render(style = SetElemsOnSeparateLines(2)))
    //println()
    graph
  }

  /**
   * Represents the root nodes, steps which do not have a dependency
   */
  lazy val roots: Seq[Step] = //theGraph.nodes.flatMap(n => if (n.edges.exists(_.node2 == n)) None else Some(n) )
    steps.filter(_.dependencies.isEmpty)

  // TODO move this to the server, allowing upgrades for all jobs on shared cluster
  private def process(dataFrame: DataFrame, step: Step): DataFrame = {
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

    val children = childrenRaw.map(n => col.getField(n).as(n))

    val columns = Seq(expr("*"), col) ++
      step.combineAuditWith.map{ fname =>
        Seq(group_results( array( col.getField("ruleSuiteResults"), scol(fname).getField("ruleSuiteResults") ) ).
          as(flowAuditColName))
      }.getOrElse(Seq.empty)

    resultApproach match {
      case AsIs => dataFrame.select(columns :_*)
      case ExpandNested => dataFrame.select(columns ++ children :_*)
      case MergeFields => // dq probably doesn't work

        val starter = dataFrame.select(columns : _*)
        val og = starter.columns.toSet + fieldName
        val nested = starter.selectExpr(s"$fieldName.result.*").columns
        starter.selectExpr((og -- nested).toSeq ++ Seq( s"$fieldName.result.*") :_*)

      case StarOnly => dataFrame.select(children: _*)
      case OutputFieldOnly => dataFrame.select(col)
    }

  }

  protected def loadViews(sparkSession: SparkSession, step: Step): ViewLoadResults = {
    // TODO ViewRow and loadConfigs should probably be public https://github.com/sparkutils/quality/issues/123
    import sparkSession.implicits._
    val (config, _) = com.sparkutils.quality.loadViewConfigs(loader = loader,
      viewDF = step.views.toDF,
      ruleSuiteIdColumn = viewColumns.ruleSuiteId,
      ruleSuiteVersionColumn = viewColumns.ruleSuiteVersion,
      ruleSuiteId = step.ruleSuite.id,
      name = viewColumns.name,
      token = viewColumns.token,
      filter = viewColumns.filter,
      sql = viewColumns.sql)
    com.sparkutils.quality.loadViews(config)
  }
// TODO - what should this be?  Probably should have a "roots" function and setup calls for each root
  /**
   * Process the data starting with the provided DataFrame registered as the first steps inputView
   * @param sparkSession
   * @param starting
   * @return
   */
  def run(sparkSession: SparkSession, starting: DataFrame): Map[String, (Step, DataFrame)] = {
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
   * @return Pairs of end Steps with their resulting DataFrames
   */
  def run(sparkSession: SparkSession): Map[String, (Step, DataFrame)] = {
    info(s"Starting Flow id: ${flowId.id}, version: ${flowId.version}")

    if (steps.nonEmpty) { // should be via graph...

      // all the paths from all the roots
      val paths = roots.map( n => n -> theGraph.get(n).outerNodeTraverser.toSeq )

      //@tailrec
      //def processRoots(root: Step, steps: Seq[Step]): (Step, DataFrame) =

      // process
      val df = steps.foldLeft(sparkSession.sql(s"select * from `${steps.head.inputView}` ")) {
        case (df, step) =>
          val vl = loadViews(sparkSession, step)
          stepViewsLoaded(vl, step)
          val starter = startStep(df, step)
          val res = process(starter, step)

          if (isDebugEnabled || showInterim) {
            infoLogStep(step, "Result Sample")
            res.show()
          }

          stepCompleted(res, step)
      }

      info(s"Finished Flow id: ${flowId.id}, version: ${flowId.version}")
      ???
    } else {
      throw FlowException("Empty Flow provided")
    }
  }

  /**
   * By default, throws errors with failedToLoadDutToCycles or if the step inputView could not be loaded
   * @param vl
   * @param step
   */
  protected def stepViewsLoaded(vl: ViewLoadResults, step: Step) = {
    if (vl.replaced.nonEmpty) {
      info(s"")
    }

    if (vl.failedToLoadDueToCycles && step.views.nonEmpty) {
      val err = FlowException(s"View Cycle - Step ${step.name}, RuleSuite id: ${flowId.id}, version: ${flowId.version} could not be started due to view cycle detection")
      error(err.msg)
      throw err
    }
    if (vl.notLoadedViews.nonEmpty) {
      if (vl.notLoadedViews.contains(step.inputView)) {
        val err = FlowException(s"View Cycle - Step ${step.name}, RuleSuite id: ${flowId.id}, version: ${flowId.version} could not be started as the Steps inputView `${step.inputView}` could not be loaded")
        error(err.msg)
        throw err
      } else {
        val msg = s"View Cycle - Step ${step.name}, RuleSuite id: ${flowId.id}, version: ${flowId.version} could not load the following views - "
        warn(msg + vl.notLoadedViews.map(v => s"`$v`").mkString)
      }
    } else {
      infoLogStep(step, "Views Loaded")
    }
  }

  /**
   * Logs at info leve the step
   * @param step
   * @param logInfo
   */
  final protected def infoLogStep(step: Step, logInfo: String): Unit = {
    info(s"Step ${step.name}, RuleSuite id: ${flowId.id}, version: ${flowId.version} $logInfo")
  }

  /**
   * By default, logs and returns input a dataframe from Step.inputViewName
   *
   * @param input either an empty dataset or the previous steps dataframe
   * @param step
   * @return the actual dataset used as input to the set
   */
  protected def startStep(input: DataFrame, step: Step): DataFrame = {
    infoLogStep(step, "Started")
    input.sparkSession.sql(s"select * from `${step.inputView}`")
  }

  /**
   * The default implementation optionally caches (cacheStepResults) and, where present, uses the view name of the next step
   * @param step
   * @return a, by default, optionally cached dataFrame
   */
  protected def stepCompleted(result: DataFrame, step: Step): DataFrame = {
    val ndf =
      if (step.cacheResults)
        result.cache
      else
        result

    ndf.createOrReplaceTempView(step.outputView)

    infoLogStep(step, "Completed")

    ndf
  }

}