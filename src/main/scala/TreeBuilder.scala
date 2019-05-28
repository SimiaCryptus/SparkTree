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

import java.util
import java.util.regex.Pattern

import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.{JsonQuery, MarkdownNotebookOutput, NotebookOutput, TableOutput}
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.repl.{SparkRepl, SparkSessionProvider}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.{Logging, ScalaJson}
import com.simiacryptus.text.{CharTrieIndex, IndexNode, TrieNode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable
import scala.util.Random

abstract class TreeBuilder extends SerializableFunction[NotebookOutput, Object] with Logging with SparkSessionProvider with InteractiveSetup[Object] {

  private lazy val tokenizer = Option(tokenizerRegex).map(Pattern.compile(_))
  val ruleSamples = 5

  val minNodeSize = 1000
  val sampleSize = 1000
  val tokenizerRegex = "\\s+"
  val maxTreeDepth: Int = 4
  val ngramLength: Int = 6
  val branchStats: Boolean = false
  val leafStats: Boolean = false
  val selectionEntropyFactor = 1.0

  override def inputTimeoutSeconds = 600

  def dataSources: Map[String, String]

  def ruleBlacklist: Array[String]

  def entropySpec(schema: StructType): Map[String, Double]

  def validationColumns: Array[String]

  def sourceTableName: String

  final def sourceDataFrame = if (spark.sqlContext.tableNames().contains(sourceTableName)) spark.sqlContext.table(sourceTableName) else null

  def statsSpec(schema: StructType): List[String]

  def extractWords(str: String): Array[String] = {
    if (tokenizer.isEmpty) Array.empty else tokenizer.get.split(str)
  }

  override def postConfigure(log: NotebookOutput): Object = {
    log.h1("Data Staging")
    log.p("""First, we will stage the initial data and manually perform a data staging query:""")
    new SparkRepl() {

      override val defaultCmd: String =
        s"""%sql
           | CREATE TEMPORARY VIEW ${sourceTableName} AS
           | SELECT * FROM ${dataSources.values.head}
        """.stripMargin.trim

      override def shouldContinue(): Boolean = {
        sourceDataFrame == null
      }

      override def init(): Unit = {
        log.run(() => {
          dataSources.foreach(t => {
            val (k, v) = t
            val frame = spark.sqlContext.read.parquet(k).persist(StorageLevel.DISK_ONLY)
            frame.createOrReplaceTempView(v)
            println(s"Loaded ${frame.count()} rows to ${v}")
          })
        })
      }
    }.apply(log)
    log.p("""This sub-report can be used for concurrent adhoc data exploration:""")
    log.subreport("explore", (sublog: NotebookOutput) => {
      val thread = new Thread(() => {
        new SparkRepl().apply(sublog)
      }: Unit)
      thread.setName("Data Exploration REPL")
      thread.setDaemon(true)
      thread.start()
      null
    })

    val Array(trainingData, testingData) = sourceDataFrame.randomSplit(Array(0.9, 0.1))
    trainingData.persist(StorageLevel.MEMORY_ONLY_SER)
    log.h1("""Tree Parameters""")
    log.p("""Now that we have loaded the schema, here are the parameters we will use for tree building:""")
    log.eval(() => {
      ScalaJson.toJson(ruleBlacklist)
    })
    log.p("Statistics Spec:")
    val objectMapper = new ObjectMapper()
      .enable(SerializationFeature.INDENT_OUTPUT)
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .enable(MapperFeature.USE_STD_BEAN_NAMING)
      .registerModule(DefaultScalaModule)
      .enableDefaultTyping()
    val statsS = new JsonQuery[List[String]](log.asInstanceOf[MarkdownNotebookOutput]).setMapper({
      objectMapper
    }).setValue(statsSpec(sourceDataFrame.schema)).print().get()
    log.p("Entropy Spec:")
    val entropyConfig = new JsonQuery[Map[String, Double]](log.asInstanceOf[MarkdownNotebookOutput]).setMapper({
      objectMapper
    }).setValue(entropySpec(sourceDataFrame.schema)).print().get()

    log.h1("Construction")
    val root = split(TreeNode(
      count = trainingData.count(),
      parent = null,
      childId = 'x',
      childRule = ""
    ), trainingData, maxDepth = maxTreeDepth, statsS = statsS, entropyConfig = entropyConfig)(log, spark)

    log.h1("Validation")
    validate(log, testingData, root)
    null
  }

  def validate(log: NotebookOutput, testingData: Dataset[Row], treeRoot: TreeNode) = {
    validationColumns.filterNot(_ == null).filterNot(_.isEmpty).foreach(columnName => {
      log.h2("Validate " + columnName)
      val totalRows = testingData.count()
      val where = treeRoot.conditions().mkString("\n AND ").replaceAll("\n", "\n  ")
      var sql = s"""SELECT *, ${treeRoot.labelingSql().replaceAll("\n", "\n  ")} AS Cluster_ID FROM $sourceTableName AS T""".stripMargin.trim
      if (sql.isEmpty) sql += s"WHERE ${where}"

      val categoryStatsPivot = spark.sql(sql).withColumnRenamed(columnName, "raw_" + columnName)
        .withColumn(columnName, concat(lit(columnName + "_"), col("raw_" + columnName)))
        .drop("raw_" + columnName)
        .groupBy("Cluster_ID")
        .pivot(columnName)
        .agg((count(lit(1))))
        .sort("Cluster_ID").cache()
      SparkRepl.out(categoryStatsPivot)(log)
      val csv = categoryStatsPivot.schema.fields.map(_.name).mkString(",") + "\n" + categoryStatsPivot.rdd.collect().map(_.toSeq.map(value => if (null != value) value.toString else "").mkString(",")).mkString("\n")
      log.p(log.file(csv, columnName + "_stats.csv", columnName + "_stats.csv"))

      testingData.schema.apply(columnName).dataType match {
        case IntegerType =>
          def getCategory(row: Row, colname: String): Integer = {
            row.getAs[Integer](colname)
          }

          def predict(partition: Iterable[Row]): Map[Integer, Double] = {
            val classes = partition.groupBy((row: Row) => getCategory(row, columnName)).mapValues(_.size)
            val total = classes.values.sum
            classes.map(e => e._1 -> e._2 / total.doubleValue())
          }

          val predictionIndex = testingData.rdd.groupBy(row => treeRoot.route(row).id).mapValues(predict(_)).collect().toMap

          val accuracy = (testingData.rdd.collect().map(row => {
            if (predictionIndex(treeRoot.route(row).id).maxBy(_._2)._1.toInt == getCategory(row, columnName).toInt) 1.0 else 0.0
          }).sum / testingData.rdd.count()).toString
          log.eval(() => {
            accuracy
          })
      }
    })
  }

  def split(treeNode: TreeNode, dataFrame: DataFrame, maxDepth: Int, statsS: List[String], entropyConfig: Map[String, Double])(implicit log: NotebookOutput, session: SparkSession): TreeNode = {
    val prevStorageLevel = dataFrame.storageLevel
    dataFrame.persist(StorageLevel.MEMORY_ONLY_SER)
    try {
      log.h2("Context")
      log.run(() => {
        println(s"Current Tree Node: ${treeNode.id}\n")
        println(treeNode.conditions().mkString("\n\tAND "))
      })
      if (maxDepth <= 0 || prune(dataFrame, entropyConfig)) {
        if (leafStats) {
          log.h2("Statistics")
          log.eval(() => {
            ScalaJson.toJson(stats(dataFrame, statsS))
          })
        }
        treeNode
      } else {
        if (branchStats) {
          log.h2("Statistics")
          log.eval(() => {
            ScalaJson.toJson(stats(dataFrame, statsS))
          })
        }
        log.h2("Rules")
        val (ruleFn, name, _, entropyDetails) = {
          val dataSample = dataFrame.sparkSession.sparkContext.broadcast(dataFrame.rdd.takeSample(false, sampleSize))
          try {
            val suggestions = ruleSuggestions(dataFrame)
            val evaluatedRules = session.sparkContext.parallelize(suggestions).map(suggestionInfo => {
              val (rule, name) = suggestionInfo
              val dataSampleValue = dataSample.value
              val entropyValues: Seq[Map[String, Any]] = dataSampleValue.groupBy(rule).map(e1 => {
                val (branchName, rows: Array[Row]) = e1
                Map("rows" -> rows.length, "branchName" -> branchName) ++ entropyConfig.toList.map(e => {
                  val (id, weight) = e
                  id -> (entropyFunction(rows, id))
                }).toMap
              }).toList
              val routeEntropy = entropy(entropyValues.toList.map(map => map("rows").asInstanceOf[Number].doubleValue().intValue()))
              val entropyMap = entropyValues.toList.map(map => {
                val rows = map("rows").asInstanceOf[Number].doubleValue()
                map("branchName") -> entropyConfig.toList.map(e => {
                  val (id, weight) = e
                  map(id).asInstanceOf[Number].doubleValue() * rows * weight
                }).sum
              }).toMap
              val fitness = entropyMap.values.sum - selectionEntropyFactor * routeEntropy * dataSampleValue.length
              (rule, name, fitness, entropyValues.map(_ ++ Map("route_entropy" -> routeEntropy, "rule_entropy" -> fitness)))
            }).collect().sortBy(-_._3)
            val head = evaluatedRules.head
            (head._1, head._2, head._3, evaluatedRules.toList.flatMap(_._4))
          } finally {
            dataSample.destroy()
          }
        }

        val entropyTable = new TableOutput()
        entropyTable.schema.put("branchName", classOf[java.lang.String])
        entropyTable.schema.put("rows", classOf[java.lang.String])
        entropyDetails.flatMap(_.keys).distinct.sorted.filterNot(entropyTable.schema.containsKey(_)).foreach(s => entropyTable.schema.put(s, classOf[java.lang.String]))
        import scala.collection.JavaConverters._
        entropyDetails.foreach(row => entropyTable.putRow(new util.HashMap[CharSequence, Object](row.map(e => e._1 -> e._2.toString).asJava)))
        log.p(entropyTable.toMarkdownTable)

        log.h2("Children")
        val partitionedData = dataFrame.rdd.groupBy(ruleFn).persist(StorageLevel.MEMORY_ONLY_SER)
        dataFrame.persist(prevStorageLevel)
        val parentWithRule = treeNode.copy(
          key = name,
          fn = ruleFn
        )
        log.eval(() => {
          ScalaJson.toJson(partitionedData.mapValues(_.size).collect().toMap)
        })
        val partitions: Array[String] = log.eval(() => {
          partitionedData.keys.distinct().collect().sorted
        })
        (for (partitionIndex <- partitions) yield {
          val frame = session.sqlContext.createDataFrame(partitionedData.filter(_._1 == partitionIndex).flatMap(_._2), dataFrame.schema)
          frame.persist(StorageLevel.MEMORY_ONLY_SER)
          val newChild = TreeNode(
            count = frame.count(),
            parent = parentWithRule,
            childId = partitions.indexOf(partitionIndex).toString.charAt(0),
            childRule = partitionIndex
          )
          log.h3("Child " + newChild.id)
          log.eval(() => {
            ScalaJson.toJson(Map(
              "rule" -> name,
              "id" -> newChild.id
            ))
          })
          val value = log.subreport(newChild.childId.toString, (child: NotebookOutput) => {
            log.write()
            split(newChild, frame, maxDepth - 1, statsS, entropyConfig)(child, session)
          })
          frame.unpersist()
          value
        }).foreach((node: TreeNode) => parentWithRule.children(node.childRule) = node)
        partitionedData.unpersist()
        log.eval(() => {
          ScalaJson.toJson(parentWithRule.children.mapValues(_.count))
        })
        log.h2("Summary")

        def extractSimpleStructure(n: TreeNode): Any = Map(
          "id" -> n.id
        ) ++ n.children.mapValues(extractSimpleStructure(_))

        log.eval(() => {
          ScalaJson.toJson(extractSimpleStructure(parentWithRule))
        })
        log.eval(() => {
          s"""SELECT *, ${parentWithRule.labelingSql().replaceAll("\n", "\n  ")} AS label
             |FROM $sourceTableName AS T
             |WHERE ${parentWithRule.conditions().mkString("\n AND ").replaceAll("\n", "\n  ")}
             |""".stripMargin.trim
        })
        parentWithRule
      }
    } finally {
      dataFrame.persist(prevStorageLevel)
    }
  }

  def stats(dataFrame: DataFrame, statsS: List[String]): Map[String, Map[String, Any]] = {
    val schema = dataFrame.schema
    schema.filter(x => statsS.contains(x.name)).map(stats(dataFrame, _)).toMap
  }

  def stats(dataFrame: DataFrame, field: StructField): (String, Map[String, Any]) = {
    val topN = 10
    val colVals = dataFrame.select(dataFrame.col(field.name)).rdd
    field.dataType match {
      case _: IntegerType =>
        val values = colVals.map(row => Option(row.getAs[Int](0))).filter(_.isDefined).map(_.get).cache()
        val cnt = values.count().doubleValue()
        val entropy = values.countByValue().values.map(_ / cnt).map(x => x * Math.log(x) / Math.log(2)).sum
        val sum0 = cnt
        val sum1 = values.sum()
        val sum2 = values.map(Math.pow(_, 2)).sum()
        val mean = sum1 / sum0
        val max = values.max()
        val min = values.min()
        values.unpersist()
        field.name -> Map(
          "max" -> max,
          "min" -> min,
          "sum0" -> sum0,
          "sum1" -> sum1,
          "sum2" -> sum2,
          "mean" -> mean,
          "stddev" -> Math.sqrt(Math.abs((sum2 / sum0) - mean * mean)),
          "common_values" -> dataFrame.rdd.map(_.getAs[Integer](field.name)).countByValue().toList.sortBy(_._2).takeRight(topN).toMap,
          "entropy" -> entropy
        )

      case _: NumericType =>
        val values = colVals.map(row => Option(row.getAs[Number](0))).filter(_.isDefined).map(_.get.doubleValue()).cache()
        val sum0 = values.map(Math.pow(_, 0)).sum()
        val sum1 = values.sum()
        val sum2 = values.map(Math.pow(_, 2)).sum()
        val mean = sum1 / sum0
        val max = values.max()
        val min = values.min()
        values.unpersist()
        field.name -> Map(
          "max" -> max,
          "min" -> min,
          "sum0" -> sum0,
          "sum1" -> sum1,
          "sum2" -> sum2,
          "mean" -> mean,
          "stddev" -> Math.sqrt(Math.abs((sum2 / sum0) - mean * mean))
        )

      case _: StringType =>
        val strings = colVals.map(row => Option(row.getAs[String](0))).filter(_.isDefined).map(_.get.toString()).cache()
        val char_entropy = entropy(index(strings.take(sampleSize)))
        val allWords = strings
          .flatMap(_.split(tokenizerRegex)).countByValue()
        val totalWords = allWords.values.sum.doubleValue()
        val word_entropy = allWords.values.map(_ / totalWords).map(x => x * Math.log(x) / Math.log(2)).sum
        val words = allWords
          .toList.sortBy(_._2).takeRight(10).toMap
        val values = strings.map(_.length).cache()
        val sum0 = values.map(Math.pow(_, 0)).sum()
        val sum1 = values.sum()
        val sum2 = values.map(Math.pow(_, 2)).sum()
        val mean = sum1 / sum0
        val max = values.max()
        val min = values.min()
        values.unpersist()
        strings.unpersist()
        field.name -> Map(
          "length" -> Map(
            "max" -> max,
            "min" -> min,
            "sum0" -> sum0,
            "sum1" -> sum1,
            "sum2" -> sum2,
            "mean" -> mean,
            "stddev" -> Math.sqrt(Math.abs((sum2 / sum0) - mean * mean))
          ),
          "char_entropy" -> char_entropy,
          "word_entropy" -> word_entropy,
          "common_words" -> words,
          "common_values" -> dataFrame.rdd.map(_.getAs[String](field.name)).countByValue().toList.sortBy(_._2).takeRight(topN).toMap
        )
    }

  }

  def index(strings: Seq[String]) = {
    if (0 >= ngramLength) null else {
      val baseTree = new CharTrieIndex
      strings.foreach(txt => baseTree.addDocument(txt))
      baseTree.index(ngramLength).root()
    }
  }

  def prune(dataFrame: DataFrame, entropyConfig: Map[String, Double]) = {
    (dataFrame.count() < minNodeSize) || (
      dataFrame.count() < 10000 && entropyConfig.toList.map(e => {
        val (id, weight) = e
        (entropyFunction(dataFrame.rdd.collect(), id)) * weight
      }).sum == 0.0)
  }

  def entropyFunction(partition: Array[Row], id: String) = {
    partition.head.schema.apply(id).dataType match {
      case _: IntegerType =>
        val classes = partition.groupBy(_.getAs[Integer](id)).mapValues(_.size)
        val values = classes.values
        entropy(values.toList)
      case _: StringType =>
        val node = index(partition.map(_.getAs[String](id)))
        val ncharEntropy = if (node != null) {
          entropy(node) / partition.length
        } else 0.0
        val wordEntropy = {
          val words = partition.flatMap(_.getAs[String](id).split(tokenizerRegex)).groupBy(x => x).mapValues(_.size)
          val values = words.values
          entropy(values.toList)
        }
        if (ncharEntropy == 0 || wordEntropy == 0) ncharEntropy + wordEntropy
        else (ncharEntropy + wordEntropy) / 2
    }
  }

  def entropy(values: Seq[Int]) = {
    val totalPop = values.sum.doubleValue()
    values.map(x => x / totalPop).map(x => x * Math.log(x) / Math.log(2)).sum
  }

  def entropy(root: IndexNode) = {
    def totalSize = root.getCursorCount

    var entropy = 0.0
    if (null != root) root.visitFirst((n: TrieNode) => {
      if (!n.hasChildren) {
        val w = n.getCursorCount.doubleValue() / totalSize
        entropy = entropy + w * Math.log(w) / Math.log(2)
      }
    })
    entropy // * totalSize
  }

  def ruleSuggestions(dataFrame: DataFrame): immutable.Seq[(Row => String, (List[String], Any))] = {
    val sampled = dataFrame.sample(false, ruleSamples.doubleValue() / dataFrame.count()).cache()
    val list = sampled.schema.filterNot(f => ruleBlacklist.contains(f.name)).flatMap(ruleSuggestions(sampled, _)).toList
    sampled.unpersist()
    list
  }

  def ruleSuggestions(dataFrame: DataFrame, field: StructField): Seq[(Row => String, (List[String], Any))] = {
    val colVals = dataFrame.select(dataFrame.col(field.name)).rdd
    field.dataType match {
      case IntegerType =>
        colVals.map(_ (0).asInstanceOf[Number].doubleValue()).distinct.collect().sorted
          .tail
          .map(value => (
            (r: Row) => java.lang.Double.compare(r.getAs[Number](field.name).doubleValue(), value) match {
              case -1 => s"""T.${field.name} < $value"""
              case 0 => s"""T.${field.name} >= $value"""
              case 1 => s"""T.${field.name} >= $value"""
            },
            List(field.name) -> value
          ))
      case StringType =>
        val sampledRows = colVals.map(_ (0).asInstanceOf[String].toString).distinct.collect().sorted
        Stream.continually({
          val words: Seq[String] = sampledRows.flatMap(str => Random.shuffle(extractWords(str).toList).take(1))
          val ngrams: Seq[String] = if (0 >= ngramLength) Seq.empty else {
            sampledRows.flatMap(str => (0 until 1).map(i => {
              if (str.length <= ngramLength) str else {
                str.drop(Random.nextInt(str.length - ngramLength)).take(ngramLength)
              }
            }))
          }
          words ++ ngrams
        }).flatten.take(1000).distinct.filter(term => {
          val matchFraction = sampledRows.filter(_.contains(term)).size / sampledRows.size.doubleValue()
          (matchFraction > 0.1) && (matchFraction < 0.8)
        }).take(ruleSamples).map(term => {
          (
            (r: Row) => r.getAs[String](field.name).contains(term) match {
              case true => s"""T.${field.name} LIKE "%${term}%""""
              case false => s"""T.${field.name} NOT LIKE "%${term}%""""
            },
            List(field.name) -> term
          )
        })
      case _ => Seq.empty
    }

  }
}
