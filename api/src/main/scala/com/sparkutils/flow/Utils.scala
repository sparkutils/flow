package com.sparkutils.flow

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, ShimUtils, functions}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.concurrent.duration.{Duration, NANOSECONDS}
import scala.util.Try

object Utils {

  def getDataType(keyName: String, config: Map[String, String]): Option[DataType] =
    config.get(keyName).map(s => DataType.fromDDL(s))

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

  /* wraps the input to ensure it can be processed in rules
  protected[flow] def wrapped(column: Column): Column =
    ShimUtils.callFunction("processor_input_wrapper", expr("*"), column)
  */
}
