package com.sparkutils.flow

import com.sparkutils.flow.StepUtils.{runnerInputs, stepTimeout}
import com.sparkutils.flow.Timer.DurationOps
import com.sparkutils.quality.{DataFrameLoader, MapConfigColumns, RuleSuiteParam, VersionedId, ViewConfigColumns}
import com.sparkutils.quality.generic.{collector, dq, engine, folder}
import com.sparkutils.quality.impl.views.ViewLoadResults
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.sparkutils.flow.impl.util.Utils._
import com.sparkutils.flow.impl.util.FlowExceptionConstants.{CycleDetected, DefaultViewNamesMultipleParents, DuplicateNames, EmptyFlow, EmptyStepName, FlowEarlyExitException, InvalidDQResultApproach, InvalidViewNames, MissingStep}
import com.sparkutils.quality.impl.mapLookup.MapTypes.MapLookups
import org.apache.spark.internal.Logging
import scalax.collection.edges.{DiEdge, DiEdgeImplicits}
import scalax.collection.immutable.Graph

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

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
 * @param flowRuleGroup top level RuleSuites to be registered
 * @tparam FG Either a simple list of Id's, or CombinedRuleSuites for flowRuleGroup usage
 */
@SerialVersionUID(1L)
class FlowT[FG, RP: RuleSuiteParam: RuleSuiteTypeParam](val flowId: VersionedId, val steps: Seq[Step[RP]],
           val flowAuditColName: String = flowAuditDefault, val duration: Duration = defaultFlowDuration,
           val flowRuleGroup: Option[FlowRuleGroup[FG]] = None,
           val loader: DataFrameLoader = new DataFrameLoader {
              override def load(token: String): DataFrame = ???
            },
           showInterim: Boolean = false, viewColumns: ViewConfigColumns = ViewConfigColumns(),
           mapColumns: MapConfigColumns = MapConfigColumns())(
             implicit ec: ExecutionContext
           ) extends Serializable with FlowDataHandling[FG, RP] with Logging {

  @transient
  lazy val theGraph: Graph[Step[RP], DiEdge[Step[RP]]] = {
    val map = steps.map(s => s.name -> s).toMap

    val graph = steps.foldLeft(scalax.collection.immutable.Graph.from[Step[RP], DiEdge[Step[RP]]](steps, Seq.empty)){
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

  private def processResult(dataFrame: DataFrame, step: Step[RP],
                            engine: RunnerOutput): (DataFrame, Duration) = Timer{
    StepUtils.processResult(dataFrame, step, engine, this)
  }

  // TODO move this to the server?, allowing upgrades for all jobs on shared cluster
  private def process(dataFrame: DataFrame, step: Step[RP]): ((DataFrame, Duration), Duration) = Timer{
    import step.operation._
    import step.options

    val fieldName = step.defaultFieldName

    val ei @ RunnerInputs(struct, withoutFlowAudit, dataRefTypeFields) = runnerInputs(options, dataFrame, step, this)

    val runner =
      function match {
        case Collect =>
          RunnerOutput(collector(step.operation.ruleSuite, resultDataType = options.dataType(resultDataType),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20),
            flatten = options.boolean(collectFlatten, true),
            includeNulls = options.boolean(collectIncludeNulls),
            useInPlaceArray = options.boolean(collectUseInPlaceArray, true),
            unrollInPlaceArray = options.boolean(collectUnrollInPlaceArray),
            unrollOutputArraySize = options.int(collectUnrollOutputArraySize, 1)).as(fieldName), Seq("ruleSuiteResults", "result"),
            dataRefTypeFields
          )

        case Engine =>
          RunnerOutput(engine(step.operation.ruleSuite,
            resultDataType = options.dataType(resultDataType),
            debugMode = options.boolean(debugMode),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20)).as(fieldName), Seq("ruleSuiteResults", "salientRule", "result"),
            dataRefTypeFields
          )

        case Folder =>
          RunnerOutput(folder(defaultFolderT(step, step.operation.ruleSuite),
            startingStruct = options.expr(startingStruct).getOrElse{
              // default to the current row - flow_audit otherwise MergeFields will have dupes
              expr(s"struct(${withoutFlowAudit.mkString(",")})")
            },
            useType = options.structType("useType").orElse(options.structType(resultDataType)),
            debugMode = options.boolean(debugMode),
            variablesPerFunc = options.int("variablesPerFunc", 40),
            variableFuncGroup = options.int("variableFuncGroup", 20)).as(fieldName), Seq("ruleSuiteResults", "result"),
            dataRefTypeFields.orElse{
              Some(withoutFlowAudit.toSet)
            }
          )

        case DQ =>
          RunnerOutput(dq(step.operation.ruleSuite).as(fieldName), Seq(), None)

        case NoOp =>
          RunnerOutput(expr("*"), Seq(), None)

        case c: CustomRunnerEngine =>
          c.customRunner(dataFrame, ei, step)
      }

    processResult(dataFrame, step, runner)

  }

  protected def loadViews(sparkSession: SparkSession, step: Step[RP]): ViewLoadResults = {
    import sparkSession.implicits._
    val (config, _) = com.sparkutils.quality.loadViewConfigs(loader = loader,
      viewDF = step.initConfiguration.viewConfig.toDF(),
      ruleSuiteId = implicitly[RuleSuiteTypeParam[RP]].id( step.operation.ruleSuite ),
      viewColumns = viewColumns)
    com.sparkutils.quality.loadViews(config)
  }

  protected def loadMaps(sparkSession: SparkSession, step: Step[RP]): MapLookups = {
    import sparkSession.implicits._
    val (config, _) = com.sparkutils.quality.loadMapConfigs(loader = loader,
      viewDF = step.initConfiguration.mapConfig.toDF(),
      ruleSuiteId = implicitly[RuleSuiteTypeParam[RP]].id( step.operation.ruleSuite ),
      mapConfig = mapColumns
    )
    step.initConfiguration.mapName.fold(
      com.sparkutils.quality.impl.mapLookup.MapLookupFunctions.loadMaps(config)
    ){ name =>
      com.sparkutils.quality.impl.mapLookup.MapLookupFunctions.loadMaps(config, name)
    }
  }

  private def performStep(df: DataFrame, step: Step[RP], dependencies: Set[StepResult[RP]]): StepResult[RP] = {
    // ensure the same session is used for the df, otherwise the session may fall back to the classic when called
    // from another thread, mostly a testing issue, but would also apply to DBR using connect on a classic cluster
    SparkSession.setActiveSession(df.sparkSession)
    try {
      val actualStep = modifyStep(df, step, dependencies)

      val vl = loadViews(df.sparkSession, actualStep)
      loadMaps(df.sparkSession, actualStep)
      stepViewsLoaded(vl, actualStep)
      val starter = startStep(df, actualStep, dependencies)
      val ((res, processed), runner) = process(starter, actualStep)

      val finalDF = stepCompleted(res, actualStep, dependencies)
      // allow early exit
      earlyExitCheck(finalDF, step, dependencies)

      // $COVERAGE-OFF$
      if (isTraceEnabled() || showInterim) {
        infoLogStep(actualStep, "Result Sample")
        finalDF.show()
      }
      // $COVERAGE-ON$

      val timings = StepTimings(runner - processed, processed)

      infoLogStep(step, s"runner took ${timings.runner.pretty} with result processing taking ${timings.result.pretty}")
      StepResult(actualStep, finalDF, timings)
    } catch {
      case f: FlowException =>
        errorLogStep(step, f.msg)
        throw StepException(step, f)
      case t: Throwable =>
        // $COVERAGE-OFF$
        val info = s"An unexpected error occurred during processing the Step - ${t.getMessage}"
        errorLogStep(step, info)
        // $COVERAGE-ON$
        throw StepException(step, FlowException(flowInfo(step, info), t))
    }
  }

  /**
   * Builds the chain of Promises from roots, completing roots triggers processing of the rest of the DAG
   * @return (roots, full) promises
   */
  private def buildPromises(): (Map[String, Promise[StepResult[RP]]], Map[String, Promise[StepResult[RP]]]) = {
    val rootPromises = roots.map(s => s.name -> Promise[StepResult[RP]]()).toMap
    val promises = scala.collection.mutable.Map.empty ++ rootPromises

    def chaseDown(elem: theGraph.NodeT): Future[StepResult[RP]] = {
      val cur = elem.source

      val r =
        promises.get(cur.name).map(_.future).getOrElse {
          val p = Promise[StepResult[RP]]()
          promises.put(cur.name, p)

          // do their roots
          val f = Future.sequence(
            elem.diPredecessors.map {
              parent =>
                chaseDown(parent)
            }
          )

          stepTimeout(cur, p)

          val newF: Future[StepResult[RP]] = f.map { names =>
            performStep(names.head.output, cur, names)
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
            case (DQ, MergeFields | OutputFieldsOnly) =>
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
   * @param tolerant defaulting to false, an exception in an Step triggers an Exception from this function, a successful
   *                 result will only have [[StepResult]]s.  Providing true will capture both successes and failures
   * @return a map of step name to tuples of end Steps with their resulting DataFrames and run statistics,
   *         note although the Step returned may have been replaced by modifyStep
   *         the name key will be the original [[Step.name]]
   */
  def run(sparkSession: SparkSession, starting: Step[RP] => Option[DataFrame], tolerant: Boolean = false): FlowResult[RP] = {
    val timed = Timer {
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

            stepTimeout(root, p)

            val r =
              df.fold(
                performStep(loadData(sparkSession = sparkSession, token = token), root, Set.empty)
              ) { df =>
                df.createOrReplaceTempView(token)
                performStep(df, root, Set.empty)
              }

            p success r
          } catch {
            case t: Throwable => p tryFailure StepException(root, t) // wrap this in an exception with the root
          }
      }

      if (tolerant) {
        val all = Future.sequence((rootPromises ++ promises).map(_._2.future.transform(Success(_))))
        val r = Await.result(all, duration)

        r.map {
          case Success(stepResult: StepResult[RP]) => stepResult.step.name -> stepResult
          case Failure(stepException: StepException[RP]) => stepException.step.name -> stepException
        }.toMap
      } else {
        val all = Future.sequence((rootPromises ++ promises).map(_._2.future))
        val r = Await.result(all, duration)

        r.map{stepResult => stepResult.step.name -> stepResult}.toMap
      }
    }
    logInfo(s"Finished Flow id: ${flowId.id}, version: ${flowId.version} in ${timed._2.pretty}")

    FlowResult(timed._1, timed._2)
  }

  /**
   * Process using the registered views using registered views as the roots, a failure in any Step
   * throws.
   *
   * @param sparkSession
   * @param starting
   * @param tolerant defaulting to false, an exception in an Step triggers an Exception from this function, a successful
   *                 result will only have [[StepResult]]s.  Providing true will capture both successes and failures
   * @return Tuples of end Steps with their resulting DataFrames and run statistics
   */
  def run(sparkSession: SparkSession, tolerant: Boolean): FlowResult[RP] =
    run(sparkSession, {
      _ => None
    }, tolerant)

  /**
   * Process using the registered views using registered views as the roots, a failure in any Step
   * throws.
   *
   * @param sparkSession
   * @param starting
   * @param tolerant defaulting to false, an exception in an Step triggers an Exception from this function, a successful
   *                 result will only have [[StepResult]]s.  Providing true will capture both successes and failures
   * @return Tuples of end Steps with their resulting DataFrames and run statistics
   */
  def run(sparkSession: SparkSession): FlowResult[RP] =
    run(sparkSession, {
      _ => None
    })

  // this logic is tested via Quality itself
  // $COVERAGE-OFF$
  /**
   * By default, throws errors with failedToLoadDutToCycles or if the step inputView could not be loaded
   * @param vl
   * @param step
   */
  private def stepViewsLoaded(vl: ViewLoadResults, step: Step[RP]) = {
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

  final def flowInfo(step: Step[RP], info: String): String =
    s"Step ${step.name}, Flow id: ${flowId.id}, version: ${flowId.version} $info"

  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final def infoLogStep(step: Step[RP], info: String): Unit = {
    logInfo(flowInfo(step,info))
  }
  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final def warnLogStep(step: Step[RP], info: String): Unit = {
    logWarning(flowInfo(step,info))
  }
  /**
   * Logs at info level the step
   * @param step
   * @param info extra logging information
   */
  final protected def errorLogStep(step: Step[RP], info: String): Unit = {
    logError(flowInfo(step,info))
  }
  // $COVERAGE-ON$

  /**
   * Called before performStep, by default just returns the step, customised Flow's can override behaviour, such as
   * generating new rules based on previous steps, or taking other actions before view loading based on either the
   * dataframe or configuration items.
   *
   * Original names will be used in the run result key but the result of this call is used in the [[run]] result pair.
   *
   * @param input
   * @param step
   * @param previousSteps
   * @return
   */
  protected def modifyStep(input: DataFrame, step: Step[RP], previousSteps: Set[StepResult[RP]]): Step[RP] =
    step

  /**
   * @inheritdoc
   */
  protected def startStep(input: DataFrame, step: Step[RP], previousSteps: Set[StepResult[RP]]): DataFrame = {
    infoLogStep(step, "Started")

    val token = step.data.inputView.getOrElse{
      // multiple are invalid, roots can have default names
      if (previousSteps.isEmpty)
        rootToken(step)
      else
        previousSteps.head.step.defaultOutputViewName
    }
    loadData(input.sparkSession, token = token)
  }

  /**
   * @inheritdoc
   */
  protected def stepCompleted(result: DataFrame, step: Step[RP], previousSteps: Set[StepResult[RP]]): DataFrame = {
    val ndf =
      if (step.data.cacheResults)
        result.cache()
      else
        result

    ndf.createOrReplaceTempView(step.defaultOutputViewName)

    infoLogStep(step, "Completed")

    ndf
  }

  /**
   * @inheritdoc
   */
  protected def earlyExitCheck(result: DataFrame, step: Step[RP], previousSteps: Set[StepResult[RP]]): Unit =
    step.options.get(flowEarlyExitSQL).foreach {
      sql =>
        val msg = step.options.getOrElse(flowEarlyExitException, FlowEarlyExitException(step))
        try {
          val s = result.sparkSession
          import s.implicits._
          val df = s.sql(sql).select(flowEarlyExitColumn).as[Boolean]
          if (!(df.isEmpty || df.head())) {
            throw FlowException(msg)
          }
        } catch {
          case f: FlowException => throw f
          case t: Throwable => throw FlowException(msg,t)
        }
    }
}