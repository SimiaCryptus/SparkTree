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
