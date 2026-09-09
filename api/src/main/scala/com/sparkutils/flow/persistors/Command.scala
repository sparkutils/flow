package com.sparkutils.flow.persistors

import com.sparkutils.flow.{Persistor, Step}
import org.apache.spark.sql.classic.DataFrame

/**
 * Runs the command template (provided in options) provided with the current dataFrame.
 * Use Command to manage 'merge into' or similar sparkSession.sql functionality.
 *
 * The dataFrame has a temporary view allocated whose identifier is provided to the named parameter 'persist_source'.
 *
 * As such any use of the persist_source must be wrapped in identifier e.g.:
 *
 * {{{
 *   merge into target_table using identifier( :persist_source ) on ...
 * }}}
 *
 * The options will be provided as named parameters to the command, it is an error to provide persist_source in the options.
 *
 * If options has 'use_result' with value 'true', the default is false, the result of the command will be rebound to the [[Step.defaultOutputViewName]]
 */
/*@SerialVersionUID(1L)
class Command() extends Persistor {

  override def persist(step: Step[_], options: Map[String, String], dataFrame: DataFrame): Option[DataFrame] = ???
}*/
