package com.sparkutils.flow.impl.util

import com.sparkutils.flow.FlowException
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, functions}

import scala.concurrent.duration.{Duration, NANOSECONDS}
import scala.reflect.ClassTag
import scala.util.Try

object Utils {

  def getDataType(keyName: String, config: Map[String, String]): Option[DataType] =
    config.get(keyName).map(s => DataType.fromDDL(s))

  def getX[T](keyName: String, config: Map[String, String], default: T)(f: String => T): T =
    config.get(keyName).map(s => Try{f(s)}.getOrElse(default)).getOrElse(default)

  def setX[T: ClassTag](keyName: String, config: Map[String, String])(f: String => T): Set[T] =
    config.get(keyName).map(s => Try{s.split(",").map(s => f(s)).toSet}.
      getOrElse(Set.empty[T])).getOrElse(Set.empty[T])


  implicit class MapOps(val config: Map[String, String]) {
    def boolean(keyName: String, default: Boolean = false): Boolean =
      getX(keyName, config, default)(_.toBoolean)

    def int(keyName: String, default: Int): Int =
      getX(keyName, config, default)(_.toInt)

    def duration(keyName: String, default: Duration): Duration =
      getX(keyName, config, default)(Duration(_))

    def dataType(keyName: String): Option[DataType] = getDataType(keyName, config)

    def strings(keyName: String): Set[String] = setX[String](keyName, config)(identity)

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

}
