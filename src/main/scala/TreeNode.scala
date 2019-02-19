/*
 * Copyright (c) 2019 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.spark.sql.Row

import scala.collection.mutable

case class TreeNode
(
  parent: TreeNode,
  count: Long,
  childId: Char,
  childRule: String,
  key: Any = "",
  fn: Row => String = null,
  children: mutable.Map[String, TreeNode] = new mutable.HashMap[String, TreeNode]()
) {
  def route(row: Row): TreeNode = {
    if (fn == null) {
      this
    } else {
      children(fn(row)).route(row)
    }
  }

  def id: String = {
    if (null == parent) "" else parent.id + childId
  }

  def labelingSql(): String = {
    if (this.children.isEmpty)
      s""""${this.id}""""
    else {
      val array = this.children.toList.sortBy(_._2.id).reverse.toArray
      var rules = array.tail.reverse.map(e => {
        s"""WHEN ${e._1} THEN ${e._2.labelingSql()}"""
      }) ++ array.headOption.map(e =>
        s"""ELSE ${e._2.labelingSql()}"""
      )
      s"""
         |CASE
         |  ${rules.mkString("\n").replaceAll("\n", "\n  ")}
         |END
           """.stripMargin.trim
    }
  }

  def conditions(): Seq[String] = {
    (
      (if (this.parent != null) this.parent.conditions() else Seq.empty)
        ++ List(this.childRule)
      ).map(_.trim).filterNot(_.isEmpty)
  }

}
