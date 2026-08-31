package com.sparkutils.flow.impl.util

import com.sparkutils.flow.{FlowException, Folder, RuleSuiteTypeParam, StepLike, StepRow, doNotAddFolderDefault}
import com.sparkutils.quality.{CombinedRuleSuiteRows, DefaultProcessor, Id, NoOpDefaultProcessor, OutputExpression, OutputExpressionRow, RuleSuite}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, functions}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.{Duration, MILLISECONDS, NANOSECONDS}
import scala.util.Try

object Utils {

  def getDataType(keyName: String, config: Map[String, String]): Option[DataType] =
    config.get(keyName).map(s => Try(DataType.fromDDL(s)).getOrElse(
      throw FlowException(s"Provided DDL `$s` is invalid")
    ))

  def getX[T](keyName: String, config: Map[String, String], default: T)(f: String => T): T =
    config.get(keyName).map(s => Try{f(s)}.getOrElse(default)).getOrElse(default)

  implicit class MapOps(val config: Map[String, String]) {
    def boolean(keyName: String, default: Boolean = false): Boolean =
      getX(keyName, config, default)(_.toBoolean)

    def int(keyName: String, default: Int): Int =
      getX(keyName, config, default)(_.toInt)

    def duration(keyName: String, default: Duration): Duration =
      getX(keyName, config, default)(Duration(_))

    def dataType(keyName: String): Option[DataType] = getDataType(keyName, config)

    def structType(keyName: String): Option[StructType] = dataType(keyName).map{
      case structType1: StructType => structType1
      case d => throw FlowException(s"DataType $d is not a StructType")
    }

    def expr(keyName: String): Option[Column] = config.get(keyName).map(s => functions.expr(s))
  }

  /**
   * Captures execution time of the thunk f
   * @param f
   * @tparam T
   * @return
   */
  def timed[T](f: => T): (T, Duration) = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()

    (ret, Duration(end - start, NANOSECONDS))
  }

  /**
   * Adds default folder logic
   * @param step
   * @param ruleSuite
   * @return
   */
  def defaultFolder(step: StepLike, ruleSuite: RuleSuite): RuleSuite =
    defaultFolder(ruleSuite, step)(_.defaultProcessor == NoOpDefaultProcessor.noOp)(
      _.copy(defaultProcessor = DefaultProcessor(Id(-1,-1), OutputExpression("row -> row"))))

  /**
   * Adds default folder logic
   *
   * @param step
   * @param ruleSuite
   * @return
   */
  def defaultFolder(step: StepLike, ruleSuite: CombinedRuleSuiteRows): CombinedRuleSuiteRows =
    defaultFolder(ruleSuite, step)(r => r.defaultProcessor.isEmpty || r.defaultProcessor.forall {
        d => Id(d.functionId, d.functionVersion) == NoOpDefaultProcessor.noOp.id
      }){ r => r.copy( defaultProcessor =
        Some(OutputExpressionRow("row -> row", -1, -1, r.ruleSuiteId, r.ruleSuiteVersion)))
      }

  /**
   * Only adds default for RuleSuite, when loading from a serialised Flow the rule may already be defaulted
   *
   * @param step
   * @param t
   * @param ev
   * @tparam T
   * @return
   */
  def defaultFolderT[T](step: StepLike, t: T)(implicit ev: RuleSuiteTypeParam[T]): T =
    t match {
      case r: RuleSuite => defaultFolder(step, r).asInstanceOf[T]
      case _ => t
    }

  /**
   * Creates a map function for combined rows
   * @param stepIds
   * @return
   */
  def defaultCombinedFolder(stepIds: Map[Id, StepRow]): (CombinedRuleSuiteRows => CombinedRuleSuiteRows) =  {
    row =>
      stepIds.get(Id(row.ruleSuiteId, row.ruleSuiteVersion)).map{ s =>
        defaultFolder(s, row)
      }.getOrElse(row)
  }

  /**
   * Adds default folder logic
   * @param step
   * @param ruleSuite
   * @return
   */
  def defaultFolder[T](t: T, step: StepLike)( isDefault: T => Boolean )( replace: T => T): T =
    (step.function, step.options.boolean(doNotAddFolderDefault, false)) match {
      case (Folder, false) =>
        if (isDefault(t))
          replace(t)
        else
          t
      case _ =>
        t
    }

  protected[flow] lazy val timer = Executors.newScheduledThreadPool(2)

  implicit class PromiseOps[T](val promise: Promise[T])(implicit executionContext: ExecutionContext) {

    /**
     * Triggers failure in the promise after duration expires, the underlying work modeled by the promise still continues
     */
    def failAfter(duration: Duration)(timedOut: => Throwable): Promise[T] = {
      val timerTask = new Runnable() {
        def run() : Unit = {
          promise.tryFailure(timedOut)
        }
      }

      timer.schedule(timerTask, duration.toMillis, MILLISECONDS)

      promise
    }

  }
}
