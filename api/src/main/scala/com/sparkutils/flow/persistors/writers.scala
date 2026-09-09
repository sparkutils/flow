package com.sparkutils.flow.persistors

import com.sparkutils.flow.impl.util.FlowExceptionConstants.MissingPersistorParameters
import com.sparkutils.flow.impl.util.Utils
import com.sparkutils.flow.{FlowException, Persistor, Step}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter}

import scala.reflect.ClassTag

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

    DataFrameWriterOptions(
      DataFrameOptions(dataFrame, options).write.format(f).mode(saveMode).options(options),
      options
    ).save(p)
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

    DataFrameWriterOptions(
      DataFrameOptions(dataFrame, options).write.mode(saveMode),
      options
    ).saveAsTable(t)
    None
  }
}

/**
 * Applies options to a type with applicable conversions or throws a nice error
 * @tparam T
 */
trait XOptions[T] {

  def just[X: ClassTag](conv: String => X)(keyName: String)(f: (T,X) => T):
    (String, (Map[String, String], T, String) => T) =
    keyName -> ((_, dfw, v) => f(dfw, Utils.orThrow[X](
      (k,v) => s"Flow expected option $k of type ${implicitly[ClassTag[X]].runtimeClass.getName} but got $v"
    )(keyName)(conv)(v)))

  val opts: Map[String, (Map[String,String], T, String) => T]

  val typ: String

  def apply(df: T, options: Map[String, String]): T = {
    val no = options.map(p => p._1.toLowerCase -> p._2.toLowerCase)
    no.keySet.intersect(opts.keySet).
      foldLeft(df){
        (df, key) =>
          opts(key)(no, df, no(key))
      }
  }

  def intPair[X](f: Seq[String] => X)(s: String): (Int, X) = {
    val b = asStrings(s)
    b.head.toInt -> f(b.tail)
  }

  /**
   * splits on ;
   */
  val asStrings: String => Array[String] = _.split(";")

  /**
   * converts options to int
   */
  val asInt: String => Int = _.toInt
  /**
   * converts options to Seq[Column] via ; separated expressions, used for coalesce etc.
   */
  val asExprs: String => Array[Column] = asStrings(_).map(expr)

  /**
   * Ignores any value configured
   */
  val asUnit: String => Unit = _ => ()

  /**
   * parses the first ; separated token as an Int, the rest as exprs
   */
  val asIntExprs: String => (Int, Seq[Column]) = intPair(_.map(expr))

  /**
   * parses the first ; separated token as an Int, the rest as strings
   */
  val asIntStrings: String => (Int, Seq[String]) = intPair(identity)
}

object DataFrameOptions extends XOptions[DataFrame] {
  override val opts: Map[String, (Map[String, String], DataFrame, String) => DataFrame] = Map(
    just(asInt)("coalesce")(_ coalesce _),
    just(asInt)("repartition_int")(_ repartition _),
    just(asIntExprs)("repartition_int_on")((d, p) => d.repartition(p._1, p._2 :_*)),
    just(asExprs)("repartition")(_ repartition(_ :_*)),
    just(asExprs)("repartition_by_range")(_ repartitionByRange(_ :_*)),
    just(asIntExprs)("repartition_int_by_range")((d, p) => d.repartitionByRange(p._1, p._2 :_*)),
    just(asIntExprs)("repartition_by_id")((d, p) => d.repartitionById(p._1, p._2.head)),
    just(asUnit)("distinct")((d, _) => d.distinct()),
  )

  override val typ: String = "DataFrame"
}

object DataFrameWriterOptions extends XOptions[DataFrameWriter[_]] {
  override val opts: Map[String, (Map[String, String], DataFrameWriter[_], String) => DataFrameWriter[_]] = Map(
    just(asStrings)("partition_by")(_ partitionBy (_ :_*)),
    just(asIntStrings)("bucket_by")((d, p) => d.bucketBy(p._1, p._2.head, p._2.tail :_*)),
    just(asStrings)("sort_by")((d, s) => d.sortBy(s.head, s.tail :_*)),
    just(asStrings)("cluster_by")((d, s) => d.clusterBy(s.head, s.tail :_*)),
  )

  override val typ: String = "DataFrameWriter"
}