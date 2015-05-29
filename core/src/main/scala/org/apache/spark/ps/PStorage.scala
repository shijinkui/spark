/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ps

import org.apache.spark.Logging
import org.apache.spark.ps.PValueType.ValueType

import scala.reflect.classTag

/**
 * local storage interface for ``org.apache.spark.ps.PSServer``
 * and ``PSTask``
 * fetch data from server, cache in local
 */
private[ps] trait PStorage[V] extends Logging {

  val vt: ValueType = classTag[V].runtimeClass.getName.toLowerCase match {
    case "long" => PValueType.LONG
    case "double" => PValueType.DOUBLE
    case "int" => PValueType.INT
  }

  /** get value from parameter server storage. */
  def get(
    tableName: String,
    key: Any,
    range: Option[Range] = None,
    seq: Option[Seq[Int]] = None): Array[_]

  /** get multi values from parameter server storage. */
  def mget(tableName: String, keys: Array): Array[Option[Array[_]]]

  /** put value into parameter server storage with specific key */
  def put(tableName: String, key: Any, value: Array[_]): Boolean

  /** put values into parameter server storage with specific key */
  def mput(tableName: String, ks: Array, vs: Array[Array[_]]): Boolean

  /** update value into parameter server storage with specific key */
  def update(tableName: String, key: Any, value: Array[_]): Boolean

  /** clear data of specific table from parameter server storage. */
  def clear(tableName: String, key: Option[Any] = None): Boolean

  /** clear current node(server or task) all data in memery and local disk */
  def clearAll(): Boolean

  /** check whether the parameter server storage contains specific key/value */
  def exists(tableName: String, key: Any): Boolean

  def mergeIntValue(origin: Int, delta: Int): Int = origin + delta

  def mergeDoubleValue(origin: Double, delta: Double): Double = origin + delta

  def mergeLongValue(origin: Long, delta: Long): Long = origin + delta
}
