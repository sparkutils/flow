package com.sparkutils.flow

import com.sparkutils.flow
import com.sparkutils.flow.impl.util.Utils
import com.sparkutils.flow.impl.util.Utils.MapOps
import com.sparkutils.quality.{DataFrameLoader, VersionedId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.Try

/**
 * Trait used to handle extra control of data before and after processing a step.
 *
 * Derive and mix in with Flow creation to allow writing to catalog's or custom storage layers
 */
trait FlowDataHandling extends Serializable with Logging { this: Flow =>

  def flowId: VersionedId

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
    step.options.get(flow.inputSchema).fold {
      val (s, t) = Utils.timed {
        input.schema
      }
      val allowed = step.options.duration(inputSchemaTimeTakenWarning, inputSchemaTimeTakenDefault)
      if (allowed.lt(t)) {
        warnLogStep(step,s", input.schema took $t, which is ${t - allowed} longer than the configured $allowed .")
      }
      s
    } { prop =>
      DataType.fromDDL(prop).asInstanceOf[StructType]
    }

  /**
   * For a root step derive the token to use when loading data
   * @param step only called for root tokens
   * @return when an inputView is provided it's used, otherwise {{step.name + rootInputSuffix}}
   */
  protected def rootToken(step: Step): String = step.data.inputView.getOrElse(step.name + rootInputSuffix)

  /**
   * By default, logs and returns input a dataframe using loadData with the token Step.inputViewName
   *
   * @param input either an empty dataset or the previous steps dataframe
   * @param step the current Step
   * @param previousSteps previous Steps, can be empty if this is a root step
   * @return the actual dataset used as input to the set
   */
  protected def startStep(input: DataFrame, step: Step, previousSteps: Set[StepResult]): DataFrame

  /**
   * The default implementation optionally caches (cacheStepResults) and uses the outputViewName to register a temp view
   * @param step the current Step
   * @param previousSteps previous Steps, can be empty if this is a root step
   * @return a, by default, optionally cached dataFrame
   */
  protected def stepCompleted(result: DataFrame, step: Step, previousSteps: Set[StepResult]): DataFrame

  /**
   * Called after stepCompleted, by default [[Constants.flowEarlyExitSQL]] logic is run, any [[Step.defaultOutputViewName]]
   * are run and available for use within the [[Step.options]] SQL configuration.
   *
   * It's results are not used by the Flow
   *
   * @param result
   * @param step
   * @param previousSteps
   */
  protected def earlyExitCheck(result: DataFrame, step: Step, previousSteps: Set[StepResult]): Unit
}