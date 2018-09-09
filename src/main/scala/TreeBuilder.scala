/*
 * Copyright (c) 2018 by Andrew Charneski.
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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.simiacryptus.sparkbook.Java8Util._
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.repl.SparkRepl
import com.simiacryptus.text.{CharTrieIndex, IndexNode, TrieNode}
import com.simiacryptus.util.io.{NotebookOutput, ScalaJson}
import com.simiacryptus.util.lang.SerializableConsumer
import org.apache.spark.sql.types.{IntegerType, NumericType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.{immutable, mutable}
import scala.util.Random


object TreeBuilder {
  val sparkSession: SparkSession = SparkSession.builder()
    .config("fs.s3a.aws.credentials.provider",
      classOf[DefaultAWSCredentialsProviderChain].getCanonicalName)
    .getOrCreate()

}

abstract class TreeBuilder extends SerializableConsumer[NotebookOutput] with Logging with InteractiveSetup {

  override def inputTimeoutSeconds = 600

  val ruleSamples = 5

  val minNodeSize = 1000

  val sampleSize = 1000

  def dataSource: String

  def ruleBlacklist: Array[String]

  def entropySpec: Map[String, Double]

  def validationColumns: Array[String]

  val tokenizerRegex = "\\s+"

  val maxTreeDepth: Int = 3

  val ngramLength: Int = 5

  val gatherNodeStatistics: Boolean = true

  def sourceTableName: String

  @transient lazy val sourceDataFrame = TreeBuilder.sparkSession.sqlContext.read.parquet(dataSource).cache()

  def statsSpec: List[String]

  override def accept2(log: NotebookOutput): Unit = {
    val dataFrame: DataFrame = log.eval(() => {
      sourceDataFrame.persist(StorageLevel.MEMORY_ONLY_SER)
      println(s"Loaded ${sourceDataFrame.count()} rows")
      sourceDataFrame
    })
    dataFrame.createOrReplaceTempView(sourceTableName)
    log.subreport("explore", (sublog: NotebookOutput) => {
      val thread = new Thread(() => new SparkRepl().accept(sublog))
      thread.setName("Data Exploration REPL")
      thread.setDaemon(true)
      thread.start()
    })
    val Array(trainingData, testingData) = dataFrame.randomSplit(Array(0.9, 0.1))
    trainingData.persist(StorageLevel.MEMORY_ONLY_SER)
    log.eval(() => {
      ScalaJson.toJson(statsSpec)
    })
    log.eval(() => {
      ScalaJson.toJson(ruleBlacklist)
    })
    log.eval(() => {
      ScalaJson.toJson(entropySpec)
    })
    val root = split(TreeNode(
      count = trainingData.count(),
      parent = null,
      childId = 'x',
      childRule = ""
    ), trainingData, maxDepth = maxTreeDepth)(log, TreeBuilder.sparkSession)
    validate(log, testingData, root)
  }

  def validate(log: NotebookOutput, testingData: Dataset[Row], treeRoot: TreeNode) = {
    validationColumns.filterNot(_ == null).filterNot(_.isEmpty).foreach(validationColumn => {
      log.h2("Validate " + validationColumn)
      testingData.schema.apply(validationColumn).dataType match {
        case IntegerType =>
          def getCategory(row: Row): Integer = {
            row.getAs[Integer](validationColumn)
          }

          def predict(partition: Iterable[Row]): Map[Integer, Double] = {
            val classes = partition.groupBy(getCategory).mapValues(_.size)
            val total = classes.values.sum
            classes.map(e => e._1 -> e._2 / total.doubleValue())
          }


          val predictionIndex: Map[String, Map[Integer, Double]] = log.eval(() => {
            testingData.rdd.groupBy(row => treeRoot.route(row).id).mapValues(predict(_)).collect().toMap
          })
          log.eval(() => {
            (testingData.rdd.collect().map(row => {
              if (predictionIndex(treeRoot.route(row).id).maxBy(_._2)._1.toInt == getCategory(row).toInt) 1.0 else 0.0
            }).sum / testingData.rdd.count()).toString
          })
      }
    })
  }

  def split(treeNode: TreeNode, dataFrame: DataFrame, maxDepth: Int)(implicit log: NotebookOutput, session: SparkSession): TreeNode = {
    val prevStorageLevel = dataFrame.storageLevel
    dataFrame.persist(StorageLevel.MEMORY_ONLY)
    try {
      log.h2("Context")
      log.run(() => {
        println(s"Current Tree Node: ${treeNode.id}\n")
        println(treeNode.conditions().mkString("\n\tAND "))
      })
      if (gatherNodeStatistics) {
        log.h2("Statistics")
        log.eval(() => {
          ScalaJson.toJson(stats(dataFrame))
        })
      }
      if (maxDepth <= 0 || prune(dataFrame)) {
        treeNode
      } else {
        log.h2("Rules")
        val dataSample = dataFrame.rdd.takeSample(false, sampleSize)
        val ruleInfo: (Row => String, (List[String], Any), Double) = log.eval(() => {
          val suggestions = ruleSuggestions(dataFrame)
          val entropyKeys = entropySpec
          val evaluatedRules = session.sparkContext.parallelize(suggestions).map(suggestionInfo => {
            val (rule, name) = suggestionInfo
            (rule, name, dataSample.groupBy(rule).mapValues(rows => {
              entropyKeys.toList.map(e => {
                val (id, weight) = e
                (entropyFunction(rows, id)) * weight
              }).sum * rows.length
            }).values.sum)
          }).collect().sortBy(-_._3)
          evaluatedRules.map(x => x._2 -> x._3).map(e => s"entropy[${e._1}] = ${e._2}\n").foreach(println)
          evaluatedRules.head
        })
        val (ruleFn, name, entropy) = ruleInfo

        log.h2("Children")
        val partitionedData = dataFrame.rdd.groupBy(ruleFn).persist(StorageLevel.MEMORY_ONLY)
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
          frame.persist(StorageLevel.MEMORY_ONLY)
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
            split(newChild, frame, maxDepth - 1)(child, session)
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

  def stats(dataFrame: DataFrame): Map[String, Map[String, Any]] = {
    dataFrame.schema.filter(x => statsSpec.contains(x.name)).map(stats(dataFrame, _)).toMap
  }

  def stats(dataFrame: DataFrame, field: StructField): (String, Map[String, Any]) = {
    val topN = 10
    val colVals = dataFrame.select(dataFrame.col(field.name)).rdd
    field.dataType match {
      case _: IntegerType =>
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
          "stddev" -> Math.sqrt(Math.abs((sum2 / sum0) - mean * mean)),
          "common_values" -> dataFrame.rdd.map(_.getAs[Integer](field.name)).countByValue().toList.sortBy(_._2).takeRight(topN).toMap
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
        val word_entropy = allWords.values.map(_ / totalWords).map(x => x * Math.log(x)).sum
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

  def prune(dataFrame: DataFrame) = {
    (dataFrame.count() < minNodeSize) || (
      dataFrame.count() < 10000 && entropySpec.toList.map(e => {
        val (id, weight) = e
        (entropyFunction(dataFrame.rdd.collect(), id)) * weight
      }).sum == 0.0)
  }

  def entropyFunction(partition: Array[Row], id: String) = {
    partition.head.schema.apply(id).dataType match {
      case _: IntegerType =>
        val classes = partition.groupBy(_.getAs[Integer](id)).mapValues(_.size)
        classes.values.map(x => x / classes.values.sum.doubleValue()).map(x => x * Math.log(x)).sum
      case _: StringType =>
        val node = index(partition.map(_.getAs[String](id)))
        if (node != null) {
          entropy(node) / partition.length
        } else {
          val words = partition.flatMap(_.getAs[String](id).split(tokenizerRegex)).groupBy(x => x).mapValues(_.size)
          words.values.map(x => x / words.values.sum.doubleValue()).map(x => x * Math.log(x)).sum
        }
    }
  }

  def entropy(root: IndexNode) = {
    val totalSize = root.getCursorCount
    var entropy = 0.0
    if (null != root) root.visitFirst((n: TrieNode) => {
      if (!n.hasChildren) {
        val w = n.getCursorCount.doubleValue() / totalSize
        entropy = entropy + w * Math.log(w)
      }
    })
    entropy * totalSize
  }

  def ruleSuggestions(dataFrame: DataFrame): immutable.Seq[(Row => String, (List[String], Any))] = {
    dataFrame.schema.filterNot(f => ruleBlacklist.contains(f.name)).flatMap(ruleSuggestions(dataFrame, _)).toStream
  }

  def ruleSuggestions(dataFrame: DataFrame, field: StructField): Seq[(Row => String, (List[String], Any))] = {
    val colVals = dataFrame.select(dataFrame.col(field.name)).rdd
    field.dataType match {
      case IntegerType =>
        colVals.map(_ (0).asInstanceOf[Number].doubleValue())
          .takeSample(false, ruleSamples)
          .distinct
          .sorted
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
        val strings = colVals.map(_ (0).asInstanceOf[String].toString).takeSample(false, 1000).toList
        Stream.continually({
          (if (tokenizerRegex.isEmpty) Seq.empty else Random.shuffle(strings).flatMap(str => Random.shuffle(str.split(tokenizerRegex).toList).take(1))) ++
            (if (0 >= ngramLength) Seq.empty else Random.shuffle(strings).flatMap(str => (0 until 1).map(i => str.drop(Random.nextInt(str.length - ngramLength)).take(ngramLength))))
        }).flatten.distinct.filter(term => {
          val matchFraction = strings.filter(_.contains(term)).size / strings.size.doubleValue()
          (matchFraction > 0.1) && (matchFraction < 0.8)
        }).take(ruleSamples).map(term => {
          (
            (r: Row) => r.getAs[String](field.name).contains(term) match {
              case true => s"""T.${field.name}.contains("${term}")"""
              case false => s"""!T.${field.name}.contains("${term}")"""
            },
            List(field.name) -> term
          )
        })
      case _ => Seq.empty
    }

  }
}
