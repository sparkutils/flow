package com.sparkutils.flow.impl.util

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Represents a shared state for ProcessBarriers indicating a stop action for all barriers, for Flow usage this implies
 * the whole flow should be stopped.
 */
case class SharedStopCondition() {
  /**
   * when non-null, it's None for it was forced to stop, or throwable if any participant in a barrier failed
   */
  private val sharedBlocker: AtomicReference[Option[Throwable]] = new AtomicReference[Option[Throwable]](null)

  def signalStop(): Unit = sharedBlocker.set(None)

  /**
   * Signals the associated ProcessBarriers to stop due to an exception.  Only the first exception raised is kept,
   * all barriers can be checked for their own thrown status
   * @param thrown
   */
  def signalStopDueToException(thrown: Throwable): Unit = sharedBlocker.compareAndSet(null, Some(thrown))

  /**
   * Was this stop condition triggered?
   * @return false and completion of all associated ProcessBarriers implies they stopped on their own
   */
  def stopped: Boolean = sharedBlocker.get() != null

  /**
   * After this condition is stopped (use isStopped first), indicates why it was stopped.
   * @return either None as signalStop was called, or a throwable triggered by one of the tasks.  A null value implies this was not stopped.
   */
  def stoppedBy: Option[Throwable] = sharedBlocker.get()
}

/**
 * Represents an operation that must be completed, with a shared optional signal to stop all processing.
 * In the case of Flow these represent each Step's Operation, allowing parts of the graph to run independent of each other.
 * @param debugHint informational when debugging
 * @param sharedCondition allows indicating that all processing should stop
 * @param duration time to await completion of this associated process
 * @tparam T
 */
class ProcessBarrier[T](val debugHint: String, sharedCondition: SharedStopCondition, duration: Duration) {
  /*
  Implementation note - it is not possible to chain Futures as execution units as these tasks cannot be started until
  their inputs are present, and these may come from any number of other futures, and as such,
  the process function must be re-entrant.
   */

  // ensure that only one thread actually runs the task
  private val lock = new ReentrantLock()
  // the underlying promise holding the resulting T
  private val result = Promise[T]()

  /**
   * True if the item is processed, threw or had a shared error or stop.
   *
   * Shared errors are not polled, so optimistically indicates the task is stopped, users of the future should
   * ensure reasonable timeouts for their awaits.
   *
   * @return
   */
  def processed(): Boolean =
    if (sharedCondition.stopped && !result.isCompleted) {
      sharedCondition.stoppedBy.exists { t =>
        try {
          result failure t
          true
        } catch {
          case _: Throwable => // in the case that there is already completed between isCompleted and starting the "then"
            true
        }
      }
    } else
      result.isCompleted

  /**
   * Future covering the execution of this barrier backed by process
   * @return
   */
  def future: Future[T] = result.future

  /**
   * Will wait until completion by another thread or try to complete in this thread.  If another thread threw,
   * this threads call will throw the same error
   * @param thunk
   * @return either Some T if there has been a previous result or None if the result is currently being processed
   *         by the same thread
   */
  def process(thunk: => T): Option[Try[T]] = {
    if (processed()) // Note as the graph can call this from multiple locations it cannot block here
      future.value
    else {
      if (lock.isHeldByCurrentThread)
        // don't recurse
        None
      else
        // we need to attempt to run it
        if (lock.tryLock())
          try{
            if (!processed()) { // The case it has been processed since trying at the start of the function
              val r = thunk
              result success r
            }
            future.value
          } catch {
            case t: Throwable =>
              result failure t
              sharedCondition signalStopDueToException t
              throw t
          } finally {
            lock.unlock()
          }
        else {
          // we need to wait for completion
          try {
            Await.result(future, duration)
          } catch {
            case t: Throwable =>
              // e.g. timeout or interrupted
              sharedCondition signalStopDueToException t
              result failure t
          }
          future.value
        }
    }
  }

}
