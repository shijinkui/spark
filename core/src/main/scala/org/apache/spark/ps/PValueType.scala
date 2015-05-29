package org.apache.spark.ps

/**
 * parameter cell value type
 */
object PValueType extends Enumeration {
  type ValueType = Value
  val INT, LONG, DOUBLE = Value
}
