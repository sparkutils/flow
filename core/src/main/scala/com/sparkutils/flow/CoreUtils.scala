package com.sparkutils.flow

import org.apache.spark.sql.DataFrame

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, durationToPair}

protected[flow] object ClassUtils {
  def load[T](obj: Object, name: String): T =
    try {
      obj.getClass.getClassLoader.loadClass(name).newInstance().asInstanceOf[T]
    } catch {
      case t: Throwable =>
        throw FlowException(s"Attempted to load CustomResultApproach $name",t)
    }
}

/**
 * A simple timer and conversion utils
 */
object Timer {

  implicit class DurationOps(val duration: Duration) {
    def pretty: String = duration match {
      case d if d.toHours > 0L => s"${duration.toHours}h${duration.toMinutes % 60}m"
      case d if d.toMinutes > 0L => s"${duration.toMinutes}m${duration.toSeconds % 60}s"
      case d if d.toSeconds > 0L => s"${duration.toSeconds}s${duration.toMillis % 1000}ms"
      case d if d.toMillis > 0L => s"${duration.toUnit(TimeUnit.MILLISECONDS)}ms"
      case d => d.toString
    }
  }

  def apply[T](thunk: => T): (T, Duration) = {
    val start = System.nanoTime()
    val r = thunk
    val duration = Duration.fromNanos( System.nanoTime() - start )
    (r, duration)
  }
}

/**
 * Implement to provide a custom persistor, by default the no-op persistor is used when creating flows
 */
trait Persistor extends Serializable {
  /**
   * @param step the Step for which the results are Persisted
   * @param options persistor specific options for processing the data
   *
   *                All option keys are lowercased before the persist call is made
   * @param dataFrame the resulting dataframe from the Step runner
   * @return None when the existing view should be used, e.g. after a potential caching,
   *         Some when the resulting dataframe should be used instead of the StepResult.
   *
   *         Where possible it's ideal to write results directly rather than cache them
   *         as writing results is typically faster and, should you wish to do this anyway,
   *         removes an additional caching step.  Examples of this would include data that
   *         writes a one-off context for all rows saved (e.g. a guuid) then re-reads the
   *         written data stopping both the expensive additional IO and calculations,
   *         with potentially differing results, if the data is not cached.
   */
  def persist(step: Step[_], options: Map[String, String], dataFrame: DataFrame): Option[DataFrame]
}

/**
 * The default persistor has no side effects.
 */
@SerialVersionUID(1L)
case object NoOpPersistor extends Persistor {
  override def persist(step: Step[_], options: Map[String, String], dataFrame: DataFrame): Option[DataFrame] = None
}