package com.sparkutils.flow.persistors

import com.sparkutils.flow.impl.util.FlowExceptionConstants.MissingPersistorParameters
import com.sparkutils.flow.{FlowException, Persistor, Step}
import org.apache.spark.sql.classic.DataFrame

/**
 * By default, appends to files - instead of the default Spark error mode, but can be configured to overwrite via save_mode.
 *
 * The following options are required: path, format
 */
@SerialVersionUID(1L)
class FileWriter() extends Persistor {

  override def persist(step: Step[_], options: Map[String, String], dataFrame: DataFrame): Option[DataFrame] = {
    val (f, p) = options.get("path").flatMap( p => options.get("format").map(f => (f, p))).
      getOrElse(throw FlowException(MissingPersistorParameters(step, classOf[FileWriter], Seq("path", "format"))))

    val saveMode = options.getOrElse("save_mode", "append")

    dataFrame.write.format(f).mode(saveMode).options(options).save(p)
    None
  }
}

/**
 * By default, appends to a catalog table, but can be configured to overwrite via save_mode.
 *
 * The following options are required: table_name
 */
@SerialVersionUID(1L)
class TableWriter() extends Persistor {

  override def persist(step: Step[_], options: Map[String, String], dataFrame: DataFrame): Option[DataFrame] = {
    val t = options.getOrElse("table_name",
      throw FlowException(MissingPersistorParameters(step, classOf[TableWriter], Seq("table_name"))))

    val saveMode = options.getOrElse("save_mode", "append")

    dataFrame.write.mode(saveMode).saveAsTable(t)
    None
  }
}