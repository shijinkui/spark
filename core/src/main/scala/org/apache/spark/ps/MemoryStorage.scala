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

import org.apache.spark.util.collection.OpenHashMap

import scala.reflect.ClassTag

/**
 * memory kv storage
 */
private[spark] class MemoryStorage[V: ClassTag] extends PStorage {

  private lazy val map = new OpenHashMap[Any, Array[_]](1000)

  override def get(
    tableName: String,
    key: Any,
    range: Option[Range],
    seq: Option[Seq[Int]]): Array[_] = {

    val ret = map(tableName + key)
    if (range.isDefined) {
      ret.slice(range.get.start, range.get.end)
    } else if (seq.isDefined) {
      val set = seq.get.toSet
      ret.zipWithIndex.filter(f => set.contains(f._2)).map(_._1)
    } else {
      ret
    }
  }

  override def update(tableName: String, key: Any, value: Array[_]): Boolean = {
    map.update(tableName + key, value)
    true
  }

  override def put(tableName: String, key: Any, value: Array[_]): Boolean = {
    map.update(tableName + key, value)
    true
  }

  override def clear(tableName: String, key: Option[Any]): Boolean = {
    if (key.isDefined) {
      map.update(tableName + key.get, Array.empty)
    } else {
      map.dropWhile(f => f._1.toString.contains(tableName))
    }

    true
  }

  override def clearAll(): Boolean = {
    map.dropWhile(f => true)
    true
  }

  override def mget(tableName: String, keys: Array): Array[Option[Array[_]]] = ???

  override def mput(tableName: String, ks: Array, vs: Array[Array[_]]): Boolean = ???

  override def exists(tableName: String, key: Any): Boolean = ???
}
