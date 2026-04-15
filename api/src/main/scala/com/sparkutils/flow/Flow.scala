package com.sparkutils.flow

import com.sparkutils.quality.{DataFrameLoader, DefaultProcessor, Id, NoOpDefaultProcessor, OutputExpression, VersionedId, ViewConfigColumns, collectRunner, ruleEngineRunner, ruleFolderRunner, typedExpressionRunner}
import com.sparkutils.quality.impl.views.ViewLoadResults
import org.apache.spark.sql.functions.{array, expr, col => scol}
import org.apache.spark.sql.{Column, DataFrame, ShimUtils, SparkSession, functions}
import Utils._
import com.sparkutils.flow.impl.util.FlowExceptionConstants.{CycleDetected, DefaultViewNamesMultipleParents, DuplicateNames, EmptyFlow, EmptyStepName, InvalidDQResultApproach, InvalidViewNames, MissingStep}
import com.sparkutils.quality.functions.{group_audit, group_results}
import com.sparkutils.quality.impl.Encoders
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DataType, StructType}
import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.immutable.Graph

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.duration.{Duration, HOURS}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
 * Represents a number of steps for processing data.  By default, views configured by token will throw not implemented,
 * provide a DataFrameLoader where tokens are used.
 *
 * @param flowId overall id for this flow, same granularity as a RuleSuiteGroup
 * @param steps steps which are processed via their DAG dependencies
 * @param flowAuditColName when enabled on a step's combineAuditWith uses this column name
 * @param duration The overall timeout to wait for completion of this Flow, by default 1hr
 * @param loader The DataFrameLoader used to handle view token loading, by default throws on any token
 * @param showInterim calls show on interim results
 * @param viewColumns columns used to process a Step's ViewRows, by default the names are those of the ViewRow columns
 * @param
 *
 */
@SerialVersionUID(1L)
class Flow(val flowId: VersionedId, val steps: Seq[Step],
           val flowAuditColName: String = flowAuditDefault, val duration: Duration = Duration(1L, HOURS),
           val loader: DataFrameLoader = new DataFrameLoader {
              override def load(token: String): DataFrame = ???
            },
           showInterim: Boolean = false, viewColumns: ViewConfigColumns = ViewConfigColumns())(
             implicit ec: ExecutionContext
           ) extends Serializable with FlowDataHandling with Logging {

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
                throw FlowException(MissingStep(step.name, dependency))
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

  private def processResult(dataFrame: DataFrame, step: Step,
                            engine: (Column, Seq[String], Option[Set[String]])): DataFrame = {
    import step.operation._
    import step.options

    val (col, childrenRaw, outputFieldsI) = engine

    val outputFields: Option[Set[String]] =
      outputFieldsI.flatMap{s =>
        if (options.boolean(forceMergeProjection))
          None
        else
          Some(s)
      }

    val struct = inputSchema(dataFrame, step)
    val withoutFlowAudit = struct.filterNot(_.name == flowAuditColName).map(_.name)

    val dataRefTypeFields =
      options.dataType(resultDataType) match {
        case Some(s: StructType) => Some(s.fields.map(_.name).filterNot(_ == flowAuditColName).toSet)
        case _ => None
      }

    val children = childrenRaw.map(n => col.getField(n).as(n))

    val (starterColumns, existingAudit) =
      // if flowAuditColName is present and the correct type, select all the others, assuming via having a parent step
      // isn't enough if StarOnly or OutputFieldOnly is provided
      //
      if (struct.exists(s => s.name == flowAuditColName && s.dataType == Encoders.ruleSuiteGroupResultsTypedEnc.catalystRepr))
        (withoutFlowAudit.map(scol) :+ col, Seq(scol(flowAuditColName)))
      else
        (Seq(expr("*"), col), Seq.empty)

    val group_auditF =
      group_audit( col,
        step.operation.combineAuditWith.map{ fnames =>
          fnames.map{fname => expr(fname)}.toSeq
        }.getOrElse(Seq.empty) ++ existingAudit
          :_*).as(flowAuditColName)

    val fieldName = step.defaultFieldName

    // if fields are present, either by default in the folder case or by providing a result type that is not an
    // array we can remove two calls to columns action, 632.73ms vs 554.88ms on using 14 chained engines 1 rule each
    // forceMergeProjection on 200,000 rows
    def withOutputFields(starter: DataFrame, outputFields: Set[String], extraFields: Set[Column] = Set.empty) =
      // cannot be run directly on dataFrame as otherwise an lca will be added to any repetitive expressions
      // this stops row number plus another of other queries running correctly in all circumstances
      starter.select(Seq(scol(fieldName), scol(flowAuditColName)) ++ extraFields ++
        outputFields.map{n =>
          scol(s"$fieldName.result.$n").as(n)
        } :_*)

    // auto add audit
    val columns = starterColumns :+ group_auditF
    resultApproach match {
      case AsIs => dataFrame.select(columns :_*)
      case ExpandNested => dataFrame.select(columns ++ children :_*)
      case MergeFields => // dq probably doesn't work

        val starter = dataFrame.select(columns: _*)
        outputFields.fold {

          val og = starter.columns.toSet
          val nested = starter.selectExpr(s"$fieldName.result.*").columns
          starter.select((og -- nested).map(scol).toSeq ++ Seq(expr(s"$fieldName.result.*")): _*)

        }{ outputFields =>

          val fields = withoutFlowAudit.toSet -- outputFields
          withOutputFields(dataFrame.select(columns: _*), outputFields, extraFields = fields.map(scol))

        }

      case OutputFieldsOnly =>

        val starter = dataFrame.select(columns: _*)

        outputFields.fold {

          val og = dataFrame.columns.toSet
          val startCols = starter.columns.toSet
          starter.select((og -- startCols).map(scol).toSeq ++
            Seq(scol(fieldName), scol(flowAuditColName), expr(s"$fieldName.result.*")): _*)

        }{ o =>
          withOutputFields(starter, outputFields = o)
        }

      case StarOnly => dataFrame.select(children: _*)
      case OutputFieldOnly => dataFrame.select(col)
    }

  }

  // TODO move this to the server?, allowing upgrades for all jobs on shared cluster
  private def process(dataFrame: DataFrame, step: Step): DataFrame = {
    import step.operation._
    import step.options

    val fieldName = step.defaultFieldName

    val struct = inputSchema(dataFrame, step)
    val withoutFlowAudit = struct.filterNot(_.name == flowAuditColName).map(_.name)

    val dataRefTypeFields =
      options.dataType(resultDataType) match {
        case Some(s: StructType) => Some(s.fields.map(_.name).filterNot(_ == flowAuditColName).toSet)
        case _ => None
      }

    val engine = (
      function.toLowerCase.replaceAll("_","") match {
        case CollectRunnerName | "collectrunner" =>
          (collectRunner(step.operation.ruleSuite, resultDataType = options.dataType(resultDataType),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20),
            flatten = options.boolean(collectFlatten, true),
            includeNulls = options.boolean(collectIncludeNulls),
            useInPlaceArray = options.boolean(collectUseInPlaceArray, true),
            unrollInPlaceArray = options.boolean(collectUnrollInPlaceArray),
            unrollOutputArraySize = options.int(collectUnrollOutputArraySize, 1)).as(fieldName), Seq("ruleSuiteResults", "result"),
            dataRefTypeFields
          )
        case EngineRunnerName | "ruleengine" | "ruleenginerunner" =>
          (ruleEngineRunner(step.operation.ruleSuite,
            resultDataType = options.dataType(resultDataType),
            debugMode = options.boolean(debugMode),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20)).as(fieldName), Seq("ruleSuiteResults", "salientRule", "result"),
            dataRefTypeFields
          )
        case FolderRunnerName | "folderrunner" =>
          val rs =
            if (step.operation.ruleSuite.defaultProcessor != NoOpDefaultProcessor.noOp)
              // even if the result is null, it's been chosen as such
              step.operation.ruleSuite
            else
              // identity function - let the row through
              step.operation.ruleSuite.copy(defaultProcessor = DefaultProcessor(Id(-1,-1), OutputExpression("row -> row")))

          (ruleFolderRunner(rs,
            startingStruct = options.expr(startingStruct).getOrElse{
              // default to the current row - flow_audit otherwise MergeFields will have dupes
              expr(s"struct(${withoutFlowAudit.mkString(",")})")
            },
            useType = options.structType("useType").orElse(options.structType(resultDataType)),
            debugMode = options.boolean(debugMode),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20)).as(fieldName), Seq("ruleSuiteResults", "result"),
            dataRefTypeFields.orElse{
              options.expr(startingStruct).flatMap(_ => None).orElse{
                Some(withoutFlowAudit.toSet)
              }
            }
          )
        case DQRunnerName | "dqrulerunner" | "rulerunner" =>
          (com.sparkutils.quality.ruleRunner(step.operation.ruleSuite).as(fieldName), Seq(), None)
      } )

    processResult(dataFrame, step, engine)

  }

  protected def loadViews(sparkSession: SparkSession, step: Step): ViewLoadResults = {
    import sparkSession.implicits._
    val (config, _) = com.sparkutils.quality.loadViewConfigs(loader = loader,
      viewDF = step.initConfiguration.viewConfig.toDF(),
      ruleSuiteIdColumn = viewColumns.ruleSuiteId,
      ruleSuiteVersionColumn = viewColumns.ruleSuiteVersion,
      ruleSuiteId = step.operation.ruleSuite.id,
      name = viewColumns.name,
      token = viewColumns.token,
      filter = viewColumns.filter,
      sql = viewColumns.sql)
    com.sparkutils.quality.loadViews(config)
  }

  private def performStep(df: DataFrame, step: Step, dependencies: Set[(Step, DataFrame)]): DataFrame = {
    // ensure the same session is used for the df, otherwise the session may fall back to the classic when called
    // from another thread, mostly a testing issue, but would also apply to DBR using connect on a classic cluster
    SparkSession.setActiveSession(df.sparkSession)
    val vl = loadViews(df.sparkSession, step)
    stepViewsLoaded(vl, step)
    val starter = startStep(df, step, dependencies)
    val res = process(starter, step)

    if (isTraceEnabled() || showInterim) {
      infoLogStep(step, "Result Sample")
      res.show()
    }

    stepCompleted(res, step, dependencies)
  }

  /**
   * Builds the chain of Promises from roots, completing roots triggers processing of the rest of the DAG
   * @return (roots, full) promises
   */
  private def buildPromises(): (Map[String, Promise[(Step, DataFrame)]], Map[String, Promise[(Step, DataFrame)]]) = {
    val rootPromises = roots.map(s => s.name -> Promise[(Step, DataFrame)]()).toMap
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
            (cur, performStep(names.head._2, cur, names))
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

  /**
   * Base implementation of step verification
   */
  def verifySteps(): Unit = {
    def badViewName(name: String): Boolean = {
      name.isEmpty
    }
    try {
      if (steps.isEmpty) {
        throw FlowException(EmptyFlow)
      }
      if (steps.exists(s => (s.name eq null) || s.name.isEmpty)) {
        throw FlowException(EmptyStepName)
      }
      steps.find(s => (s.data.inputView.exists(badViewName) || s.data.inputView.isEmpty) && s.dependencies.size > 1).foreach {
        step =>
          throw FlowException(DefaultViewNamesMultipleParents(step))
      }
      steps.find(s => s.data.inputView.exists(badViewName) || s.data.outputView.exists(badViewName)).foreach {
        step =>
          throw FlowException(InvalidViewNames(step))
      }
      val dupes = steps.groupBy(_.name).filter(_._2.size > 1)
      if (dupes.nonEmpty) {
        throw FlowException(s"$DuplicateNames (${dupes.keys.mkString(",")})")
      }
      val cyc = theGraph.findCycle
      // for info only val paths = roots.map( n => n -> theGraph.get(n).outerNodeTraverser.toSeq )
      cyc.foreach {
        cycle =>
          throw FlowException(s"$CycleDetected $cycle")
      }

      steps.foreach {
        step =>
          (step.operation.function, step.operation.resultApproach) match {
            case (DQRunnerName | "dqrulerunner" | "rulerunner", MergeFields | OutputFieldsOnly) =>
              throw FlowException(InvalidDQResultApproach(step))
            case _ => ()
          }
      }
    } catch {
      case f: FlowException =>
        logError(f.msg, f)
        throw f
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
   *                 inputView is not present an error is thrown, this is delegated to the FlowDataHandling.loadData function
   * @return
   */
  def run(sparkSession: SparkSession, starting: Step => Option[DataFrame]): Map[String, (Step, DataFrame)] = {
    verifySteps()
    logInfo(s"Starting Flow id: ${flowId.id}, version: ${flowId.version}")

    val (rootPromises, promises) = buildPromises()

    // processing th roots sets of the rest
    roots.par.map {
      root =>
        val p = rootPromises(root.name)
        try {
          val df = starting(root)
          val token = rootToken(root)
          val r = (root,
            df.fold(
              performStep( loadData(sparkSession = sparkSession, token = token), root, Set.empty)
            ) { df =>
              df.createOrReplaceTempView(token)
              performStep(df, root, Set.empty)
            }
          )
          p success r
        } catch {
          case t: Throwable => p failure t
        }
    }

    val all = Future.sequence(promises.map(_._2.future))
    val r = Await.result(all, duration)

    logInfo(s"Finished Flow id: ${flowId.id}, version: ${flowId.version}")
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
  private def stepViewsLoaded(vl: ViewLoadResults, step: Step) = {
    if (vl.replaced.nonEmpty) {
      logInfo(s"Views in Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} were replaced `${vl.replaced.mkString(",")}`")
    }

    if (vl.failedToLoadDueToCycles && step.initConfiguration.viewConfig.nonEmpty) {
      val err = FlowException(s"View Cycle - Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} could not be started due to view cycle detection")
      logError(err.msg, err)
      throw err
    }
    if (vl.notLoadedViews.nonEmpty) {
      if (vl.notLoadedViews.exists(step.data.inputView.contains)) {
        val err = FlowException(s"View Cycle - Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} could not be started as the Steps inputView `${step.data.inputView}` could not be loaded")
        logError(err.msg, err)
        throw err
      } else {
        val msg = s"View Cycle - Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} could not load the following views - "
        logWarning(msg + vl.notLoadedViews.map(v => s"`$v`").mkString)
      }
    } else {
      infoLogStep(step, "Views Loaded")
    }
  }

  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final def infoLogStep(step: Step, info: String): Unit = {
    logInfo(s"Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} $info")
  }
  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final def debugLogStep(step: Step, info: String): Unit = {
    logInfo(s"Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} $info")
  }
  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final def traceLogStep(step: Step, info: String): Unit = {
    logTrace(s"Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} $info")
  }
  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final def warnLogStep(step: Step, info: String): Unit = {
    logWarning(s"Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} $info")
  }
  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final protected def errorLogStep(step: Step, info: String): Unit = {
    logError(s"Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} $info")
  }

  /**
   * By default, logs and returns input a dataframe via FlowDataHandling.loadData using Step.inputViewName as the token
   *
   * @param input either an empty dataset or the previous steps dataframe, by default this is ignored
   * @param step
   * @return the actual dataset used as input to the set
   */
  protected def startStep(input: DataFrame, step: Step, previousSteps: Set[(Step, DataFrame)]): DataFrame = {
    infoLogStep(step, "Started")

    val token = step.data.inputView.getOrElse{
      // multiple are invalid, roots can have default names
      if (previousSteps.isEmpty)
        rootToken(step)
      else
        previousSteps.head._1.defaultOutputViewName
    }
    loadData(input.sparkSession, token = token)
  }

  /**
   * The default implementation optionally caches (cacheStepResults) and uses the outputViewName to register a temp view
   * @param step
   * @return a, by default, optionally cached dataFrame
   */
  protected def stepCompleted(result: DataFrame, step: Step, previousSteps: Set[(Step, DataFrame)]): DataFrame = {
    val ndf =
      if (step.data.cacheResults)
        result.cache()
      else
        result

    ndf.createOrReplaceTempView(step.defaultOutputViewName)

    infoLogStep(step, "Completed")

    ndf
  }

}