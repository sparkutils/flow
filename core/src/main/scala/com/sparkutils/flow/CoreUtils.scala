package com.sparkutils.flow

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


