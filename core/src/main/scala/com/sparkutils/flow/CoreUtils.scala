package com.sparkutils.flow

protected[flow] object ClassUtils {
  def load[T](obj: Object, name: String): T =
    try {
      obj.getClass.getClassLoader.loadClass(name).newInstance().asInstanceOf[T]
    } catch {
      case t: Throwable =>
        throw FlowException(s"Attempted to load CustomResultApproach $name",t)
    }
}

