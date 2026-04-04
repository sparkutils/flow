package com.sparkutils.flow

import com.sparkutils.quality.{DataFrameLoader, DefaultProcessor, Id, NoOpDefaultProcessor, OutputExpression, VersionedId, collectRunner, ruleEngineRunner, ruleFolderRunner, typedExpressionRunner}
import com.sparkutils.quality.impl.views.ViewLoadResults
import org.apache.spark.sql.functions.{array, expr, col => scol}
import org.apache.spark.sql.{Column, DataFrame, ShimUtils, SparkSession}
import Utils._
import com.sparkutils.quality.functions.group_results
import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.immutable.Graph

import scala.concurrent.duration.{Duration, HOURS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

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
           val flowAuditColName: String = "flow_audit", val duration: Duration = Duration(1L, HOURS),
           loader: DataFrameLoader = new DataFrameLoader {
              override def load(token: String): DataFrame = ???
            },
           showInterim: Boolean = false, viewColumns: ViewConfigColumns = ViewConfigColumns())(
             implicit ec: ExecutionContext
           ) extends Serializable {
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
  private val (roots, rest) = steps.partition(_.dependencies.isEmpty)

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

  private def performStep(df: DataFrame, step: Step): DataFrame = {
    // ensure the same session is used for the df, otherwise the session may fall back to the classic when called
    // from another thread, mostly a testing issue, but would also apply to DBR using connect on a classic cluster
    SparkSession.setActiveSession(df.sparkSession)
    val vl = loadViews(df.sparkSession, step)
    stepViewsLoaded(vl, step)
    val starter = startStep(df, step)
    val res = process(starter, step)

    if (isDebugEnabled || showInterim) {
      infoLogStep(step, "Result Sample")
      res.show()
    }

    stepCompleted(res, step)
  }

  /**
   * Builds the chain of Promises from roots, completing roots triggers processing of the rest of the DAG
   * @return (roots, full) promises
   */
  private def buildPromises(): (Map[String, Promise[(Step, DataFrame)]], Map[String, Promise[(Step, DataFrame)]]) = {
    val rootPromises = roots.map(s => s.name -> Promise[(Step, DataFrame)]).toMap
    val promises = scala.collection.mutable.Map.empty ++ rootPromises

    def chaseDown(elem: theGraph.NodeT): Future[(Step, DataFrame)] = {
      val cur = elem.source

      val r =
        promises.get(cur.name).map(_.future).getOrElse {
          val p = Promise[(Step, DataFrame)]()
          promises.put(cur.name, p)

          // do their roots
          val f = Future.sequence(
            elem.diPredecessors.map {
              parent =>
                chaseDown(parent)
            }
          )

          val newF: Future[(Step, DataFrame)] = f.map { names =>
            (cur, performStep(names.head._2, cur))
          }

          p.completeWith(newF)
          p.future
        }

      // children should stitch on their parent only
      elem.diSuccessors.foreach {
        dependent =>
          chaseDown(dependent)
      }

      r
    }
    rest.foreach{r => chaseDown(theGraph.get(r))}

    (rootPromises, scala.collection.immutable.Map.empty ++ promises)
  }

  private def verifySteps(): Unit = {
    if (steps.isEmpty) {
      throw FlowException("Empty Flow provided")
    }
    val dupes = steps.groupBy(_.name).filter(_._2.size > 1)
    if (dupes.nonEmpty) {
      throw FlowException(s"Flow provided Steps with duplicate names ${dupes.keys.mkString(",")}")
    }
  }

  /**
   * A convenience function to start processing with an optionally provided DataFrame registered as the first steps inputView.
   *
   * The entire Flow must be complete within the Flow.duration
   *
   * @param sparkSession
   * @param starting a function called for each Root Step (a Step having no dependencies), when a DataFrame is provided
   *                 it is registered as a temporary view with the name of the inputView.  When None is returned and the
   *                 inputView is not present an error is thrown
   * @return
   */
  def run(sparkSession: SparkSession, starting: Step => Option[DataFrame]): Map[String, (Step, DataFrame)] = {
    verifySteps()
    info(s"Starting Flow id: ${flowId.id}, version: ${flowId.version}")

    val (rootPromises, promises) = buildPromises()

    // processing th roots sets of the rest
    roots.map {
      root =>
        val p = rootPromises(root.name)
        try {
          val df = starting(root)
          val r = (root,
            df.fold(
              performStep( sparkSession.sql(s"select * from `${root.inputView}` "), root)
            ) { df =>
              df.createOrReplaceTempView(root.inputView)
              performStep(df, root)
            }
          )
          p success r
        } catch {
          case t: Throwable => p failure t
        }
    }

    val all = Future.sequence(promises.map(_._2.future))
    val r = Await.result(all, duration)

    info(s"Finished Flow id: ${flowId.id}, version: ${flowId.version}")
    r.map(p => p._1.name -> p).toMap
  }

  /**
   * Process using the registered views using registered views as the roots
   * @param sparkSession
   * @param starting
   * @return Pairs of end Steps with their resulting DataFrames
   */
  def run(sparkSession: SparkSession): Map[String, (Step, DataFrame)] =
    run(sparkSession, {
      _ => None
    })

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