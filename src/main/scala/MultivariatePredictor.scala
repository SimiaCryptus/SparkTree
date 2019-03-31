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

import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.repl.{SparkRepl, SparkSessionProvider}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

abstract class MultivariatePredictor extends SerializableFunction[NotebookOutput, Object] with Logging with SparkSessionProvider with InteractiveSetup[Object] {

  val targetCol = "Cover_Type"

  override def inputTimeoutSeconds = 600

  def dataSources: Map[String, String]

  def sourceTableName: String

  override def postConfigure(log: NotebookOutput): Object = {
    log.h1("Data Staging")
    log.p("""First, we will stage the initial data and manually perform a data staging query:""")
    new SparkRepl() {

      override val defaultCmd: String =
        s"""%sql
           | CREATE TEMPORARY VIEW $sourceTableName AS
           | SELECT
           |  T.*,
           |  ${functions.map(t => t._2 + " AS " + t._1).mkString(", ")}
           | FROM ${dataSources.values.head} AS T
           |        """.stripMargin

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

    for (sourceCol <- functions.keys) {
      val predict_matrix = predictiveMatrix(sourceCol)
      predict_matrix.createOrReplaceTempView(s"${sourceCol}_$targetCol")
      SparkRepl.out(predict_matrix)(log)
    }
    val possibleClasses = sourceDataFrame.select(targetCol).rdd.map(_.getAs[Object](0).toString).distinct().collect()
    spark.sqlContext.udf.register("max", (a: Double, b: Double) => if (null == a) b else if (null == b) a else Math.max(a, b))

    new SparkRepl() {

      val maxExpr = possibleClasses.map(category => {
        s"IFNULL(T.${targetCol}_${category},0)"
      }).reduce((a, b) => s"max($a,$b)")

      val predictiveFn = "CASE " + possibleClasses.map(category => {
        s"WHEN T.${targetCol}_${category} == T.max THEN '${category}'"
      }).mkString("\n  ") + " END"

      override val defaultCmd: String = {
        val predictData = s"SELECT T.${targetCol}, " + possibleClasses.map(category => {
          "(" + functions.keys.map(inputCol => {
            s"T_$inputCol.${targetCol}_${category}"
          }).mkString(" * ") + ")" + " AS " + s"${targetCol}_${category}"
        }).mkString(",\n  ") + s"\nFROM ${sourceTableName} AS T \n" + functions.keys.map(inputCol => {
          s"INNER JOIN ${inputCol}_$targetCol AS T_${inputCol} ON T_${inputCol}.${inputCol} = T.${inputCol}"
        }).mkString("\n  ")
        s"""%sql
           |CREATE TEMPORARY VIEW predict AS
           |${predictData};
           |
           |CREATE TEMPORARY VIEW predict_mag AS
           |SELECT *, ${possibleClasses.map(category => s"IFNULL(${targetCol}_${category},0.0)").mkString(" + ")} AS magnitude
           |FROM predict;
           |
           |CREATE TEMPORARY VIEW predict_normalized AS
           |SELECT ${targetCol}, ${possibleClasses.map(category => s"${targetCol}_${category} / magnitude AS ${targetCol}_${category}").mkString(",\n")}
           |FROM predict_mag;
           |
           |CREATE TEMPORARY VIEW predict_with_max AS
           |SELECT *, $maxExpr AS max FROM predict_normalized AS T;
           |
           |CREATE TEMPORARY VIEW predict_with_label AS
           |SELECT *, $predictiveFn AS predict FROM predict_with_max AS T;
           |
           |WITH A AS (
           | SELECT COUNT(1) AS V
           | FROM predict_with_label
           | WHERE $targetCol == predict
           |),
           |B AS (
           | SELECT COUNT(1) AS V
           | FROM predict_with_label
           | WHERE $targetCol != predict
           |)
           |SELECT A.V / (A.V + B.V) AS accuracy FROM A CROSS JOIN B
           |;
           |
 |
         """.stripMargin
      }

      override def shouldContinue(): Boolean = {
        true
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


    null
  }

  final def sourceDataFrame: DataFrame = if (spark.sqlContext.tableNames().contains(sourceTableName)) spark.sqlContext.table(sourceTableName).cache() else null

  def functions = Map(
    "label_a" ->
      """CASE
        |    WHEN T.Elevation < 2996.0 THEN CASE
        |      WHEN T.Elevation < 2736.0 THEN CASE
        |        WHEN T.Elevation < 2562.0 THEN CASE
        |          WHEN T.Elevation < 2435.0 THEN CASE
        |            WHEN T.Soil_Description LIKE "%Bullwark%" THEN CASE
        |              WHEN T.Hillshade_9am < 168.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 700.0 THEN CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 61.0 THEN "00000000"
        |                  ELSE "00000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 27.0 THEN "00000010"
        |                  ELSE "00000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 582.0 THEN CASE
        |                  WHEN T.Elevation < 2262.0 THEN "00000100"
        |                  ELSE "00000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 2323.0 THEN "00000110"
        |                  ELSE "00000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 633.0 THEN CASE
        |                WHEN T.Hillshade_9am < 225.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 646.0 THEN "00001000"
        |                  ELSE "00001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 150.0 THEN "00001010"
        |                  ELSE "00001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 2314.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%stony.%" THEN "00001100"
        |                  ELSE "00001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 2376.0 THEN "00001110"
        |                  ELSE "00001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Elevation < 2514.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 134.0 THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "00010000"
        |                  ELSE "00010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 2475.0 THEN "00010010"
        |                  ELSE "00010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 2483.0 THEN CASE
        |                  WHEN T.Slope < 14.0 THEN "00010100"
        |                  ELSE "00010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "00010110"
        |                  ELSE "00010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |                WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 875.0 THEN "00011000"
        |                  ELSE "00011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1920.0 THEN "00011010"
        |                  ELSE "00011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1103.0 THEN "00011100"
        |                  ELSE "00011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_4 < 1.0 THEN "00011110"
        |                  ELSE "00011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |            WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 1572.0 THEN CASE
        |                WHEN T.Slope < 18.0 THEN CASE
        |                  WHEN T.Soil_Type_11 < 1.0 THEN "00100000"
        |                  ELSE "00100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 2649.0 THEN "00100010"
        |                  ELSE "00100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1567.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 892.0 THEN "00100100"
        |                  ELSE "00100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2483.0 THEN "00100110"
        |                  ELSE "00100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Ratake%" THEN CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 242.0 THEN CASE
        |                  WHEN T.Hillshade_3pm < 150.0 THEN "00101000"
        |                  ELSE "00101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_Noon < 236.0 THEN "00101010"
        |                  ELSE "00101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%complex.%" THEN CASE
        |                  WHEN T.Soil_Type_17 < 1.0 THEN "00101100"
        |                  ELSE "00101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%extremely%" THEN "00101110"
        |                  ELSE "00101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Horizontal_Distance_To_Hydrology < 170.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%Como%" THEN CASE
        |                WHEN T.Hillshade_9am < 227.0 THEN CASE
        |                  WHEN T.Elevation < 2671.0 THEN "00110000"
        |                  ELSE "00110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 2661.0 THEN "00110010"
        |                  ELSE "00110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 2690.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1516.0 THEN "00110100"
        |                  ELSE "00110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2108.0 THEN "00110110"
        |                  ELSE "00110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Hydrology < 272.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%Como%" THEN CASE
        |                  WHEN T.Elevation < 2660.0 THEN "00111000"
        |                  ELSE "00111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 21.0 THEN "00111010"
        |                  ELSE "00111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Como%" THEN CASE
        |                  WHEN T.Aspect < 65.0 THEN "00111100"
        |                  ELSE "00111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1050.0 THEN "00111110"
        |                  ELSE "00111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |      ELSE CASE
        |        WHEN T.Elevation < 2902.0 THEN CASE
        |          WHEN T.Horizontal_Distance_To_Roadways < 1950.0 THEN CASE
        |            WHEN T.Vertical_Distance_To_Hydrology < 32.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%family%" THEN CASE
        |                WHEN T.Hillshade_9am < 219.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%complex,%" THEN "01000000"
        |                  ELSE "01000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1188.0 THEN "01000010"
        |                  ELSE "01000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 2060.0 THEN CASE
        |                  WHEN T.Elevation < 2817.0 THEN "01000100"
        |                  ELSE "01000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 108.0 THEN "01000110"
        |                  ELSE "01000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 1128.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                  WHEN T.Soil_Description LIKE "%land%" THEN "01001000"
        |                  ELSE "01001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 382.0 THEN "01001010"
        |                  ELSE "01001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 18.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2047.0 THEN "01001100"
        |                  ELSE "01001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 194.0 THEN "01001110"
        |                  ELSE "01001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Soil_Description LIKE "%Legault%" THEN CASE
        |              WHEN T.Hillshade_9am < 223.0 THEN CASE
        |                WHEN T.Aspect < 223.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2894.0 THEN "01010000"
        |                  ELSE "01010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2684.0 THEN "01010010"
        |                  ELSE "01010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Type_12 < 1.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 127.0 THEN "01010100"
        |                  ELSE "01010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2732.0 THEN "01010110"
        |                  ELSE "01010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Leighcan%" THEN CASE
        |                WHEN T.Soil_Description LIKE "%family,%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2249.0 THEN "01011000"
        |                  ELSE "01011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_33 < 1.0 THEN "01011010"
        |                  ELSE "01011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 2509.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 201.0 THEN "01011100"
        |                  ELSE "01011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 212.0 THEN "01011110"
        |                  ELSE "01011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Vertical_Distance_To_Hydrology < 31.0 THEN CASE
        |            WHEN T.Horizontal_Distance_To_Hydrology < 124.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%complex,%" THEN CASE
        |                WHEN T.Aspect < 120.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2546.0 THEN "01100000"
        |                  ELSE "01100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 9.0 THEN "01100010"
        |                  ELSE "01100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 2952.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2984.0 THEN "01100100"
        |                  ELSE "01100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2177.0 THEN "01100110"
        |                  ELSE "01100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Elevation < 2949.0 THEN CASE
        |                WHEN T.Hillshade_Noon < 231.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Leighcan%" THEN "01101000"
        |                  ELSE "01101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 3445.0 THEN "01101010"
        |                  ELSE "01101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Legault%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 255.0 THEN "01101100"
        |                  ELSE "01101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%Catamount%" THEN "01101110"
        |                  ELSE "01101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Horizontal_Distance_To_Roadways < 2713.0 THEN CASE
        |              WHEN T.Hillshade_Noon < 224.0 THEN CASE
        |                WHEN T.Hillshade_Noon < 207.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 342.0 THEN "01110000"
        |                  ELSE "01110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1595.0 THEN "01110010"
        |                  ELSE "01110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_Noon < 241.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1702.0 THEN "01110100"
        |                  ELSE "01110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 216.0 THEN "01110110"
        |                  ELSE "01110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 231.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 4218.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Como%" THEN "01111000"
        |                  ELSE "01111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 75.0 THEN "01111010"
        |                  ELSE "01111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 366.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Rock%" THEN "01111100"
        |                  ELSE "01111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 488.0 THEN "01111110"
        |                  ELSE "01111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |    END
        |    ELSE CASE
        |      WHEN T.Elevation < 3160.0 THEN CASE
        |        WHEN T.Horizontal_Distance_To_Hydrology < 247.0 THEN CASE
        |          WHEN T.Soil_Description LIKE "%complex,%" THEN CASE
        |            WHEN T.Hillshade_Noon < 226.0 THEN CASE
        |              WHEN T.Elevation < 3063.0 THEN CASE
        |                WHEN T.Slope < 15.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 150.0 THEN "10000000"
        |                  ELSE "10000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_29 < 1.0 THEN "10000010"
        |                  ELSE "10000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_Noon < 217.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 153.0 THEN "10000100"
        |                  ELSE "10000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 3108.0 THEN "10000110"
        |                  ELSE "10000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Elevation < 3073.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1500.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Leighcan%" THEN "10001000"
        |                  ELSE "10001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 10.0 THEN "10001010"
        |                  ELSE "10001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 3744.0 THEN "10001100"
        |                  ELSE "10001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_32 < 1.0 THEN "10001110"
        |                  ELSE "10001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Vertical_Distance_To_Hydrology < 8.0 THEN CASE
        |              WHEN T.Elevation < 3080.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1471.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 904.0 THEN "10010000"
        |                  ELSE "10010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2407.0 THEN "10010010"
        |                  ELSE "10010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 2201.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1783.0 THEN "10010100"
        |                  ELSE "10010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1664.0 THEN "10010110"
        |                  ELSE "10010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 227.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 2100.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1048.0 THEN "10011000"
        |                  ELSE "10011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 208.0 THEN "10011010"
        |                  ELSE "10011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_Noon < 239.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1442.0 THEN "10011100"
        |                  ELSE "10011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1998.0 THEN "10011110"
        |                  ELSE "10011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Hillshade_Noon < 228.0 THEN CASE
        |            WHEN T.Elevation < 3078.0 THEN CASE
        |              WHEN T.Elevation < 3032.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 408.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 215.0 THEN "10100000"
        |                  ELSE "10100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_Noon < 213.0 THEN "10100010"
        |                  ELSE "10100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 446.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 216.0 THEN "10100100"
        |                  ELSE "10100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 16.0 THEN "10100110"
        |                  ELSE "10100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Fire_Points < 1966.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%family%" THEN CASE
        |                  WHEN T.Soil_Description LIKE "%family,%" THEN "10101000"
        |                  ELSE "10101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1233.0 THEN "10101010"
        |                  ELSE "10101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 2335.0 THEN CASE
        |                  WHEN T.Aspect < 49.0 THEN "10101100"
        |                  ELSE "10101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 3034.0 THEN "10101110"
        |                  ELSE "10101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Elevation < 3071.0 THEN CASE
        |              WHEN T.Horizontal_Distance_To_Hydrology < 492.0 THEN CASE
        |                WHEN T.Elevation < 3032.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Rock%" THEN "10110000"
        |                  ELSE "10110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2554.0 THEN "10110010"
        |                  ELSE "10110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 3030.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2831.0 THEN "10110100"
        |                  ELSE "10110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 9.0 THEN "10110110"
        |                  ELSE "10110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 240.0 THEN CASE
        |                WHEN T.Elevation < 3117.0 THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "10111000"
        |                  ELSE "10111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 220.0 THEN "10111010"
        |                  ELSE "10111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Rock%" THEN CASE
        |                  WHEN T.Aspect < 223.0 THEN "10111100"
        |                  ELSE "10111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1624.0 THEN "10111110"
        |                  ELSE "10111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |      ELSE CASE
        |        WHEN T.Elevation < 3269.0 THEN CASE
        |          WHEN T.Elevation < 3213.0 THEN CASE
        |            WHEN T.Horizontal_Distance_To_Hydrology < 268.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%family,%" THEN CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 120.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2974.0 THEN "11000000"
        |                  ELSE "11000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1682.0 THEN "11000010"
        |                  ELSE "11000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 150.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 222.0 THEN "11000100"
        |                  ELSE "11000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2742.0 THEN "11000110"
        |                  ELSE "11000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 227.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 1967.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1061.0 THEN "11001000"
        |                  ELSE "11001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 14.0 THEN "11001010"
        |                  ELSE "11001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_Noon < 237.0 THEN CASE
        |                  WHEN T.Slope < 8.0 THEN "11001100"
        |                  ELSE "11001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1974.0 THEN "11001110"
        |                  ELSE "11001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Horizontal_Distance_To_Hydrology < 301.0 THEN CASE
        |              WHEN T.Horizontal_Distance_To_Fire_Points < 1668.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%substratum%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 4384.0 THEN "11010000"
        |                  ELSE "11010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "11010010"
        |                  ELSE "11010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 2430.0 THEN CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "11010100"
        |                  ELSE "11010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_3pm < 144.0 THEN "11010110"
        |                  ELSE "11010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 229.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 1929.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1061.0 THEN "11011000"
        |                  ELSE "11011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "11011010"
        |                  ELSE "11011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 454.0 THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "11011100"
        |                  ELSE "11011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 11.0 THEN "11011110"
        |                  ELSE "11011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Elevation < 3329.0 THEN CASE
        |            WHEN T.Elevation < 3296.0 THEN CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 1910.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 2165.0 THEN CASE
        |                  WHEN T.Wilderness_Area_2 < 1.0 THEN "11100000"
        |                  ELSE "11100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "11100010"
        |                  ELSE "11100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_Noon < 231.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Moran%" THEN "11100100"
        |                  ELSE "11100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 34.0 THEN "11100110"
        |                  ELSE "11100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Moran%" THEN CASE
        |                WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |                  WHEN T.Soil_Type_39 < 1.0 THEN "11101000"
        |                  ELSE "11101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2166.0 THEN "11101010"
        |                  ELSE "11101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1964.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2368.0 THEN "11101100"
        |                  ELSE "11101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1879.0 THEN "11101110"
        |                  ELSE "11101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |              WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%Moran%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2643.0 THEN "11110000"
        |                  ELSE "11110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 3368.0 THEN "11110010"
        |                  ELSE "11110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1991.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 213.0 THEN "11110100"
        |                  ELSE "11110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 300.0 THEN "11110110"
        |                  ELSE "11110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 2024.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2320.0 THEN "11111000"
        |                  ELSE "11111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2648.0 THEN "11111010"
        |                  ELSE "11111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Cryaquolls%" THEN CASE
        |                  WHEN T.Hillshade_Noon < 243.0 THEN "11111100"
        |                  ELSE "11111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 3401.0 THEN "11111110"
        |                  ELSE "11111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |    END
        |  END""".stripMargin,
    "label_b" ->
      """CASE
        |    WHEN T.Elevation < 2986.0 THEN CASE
        |      WHEN T.Elevation < 2775.0 THEN CASE
        |        WHEN T.Soil_Description LIKE "%outcrop%" THEN CASE
        |          WHEN T.Elevation < 2499.0 THEN CASE
        |            WHEN T.Horizontal_Distance_To_Fire_Points < 711.0 THEN CASE
        |              WHEN T.Vertical_Distance_To_Hydrology < 45.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 85.0 THEN CASE
        |                  WHEN T.Elevation < 2276.0 THEN "00000000"
        |                  ELSE "00000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 127.0 THEN "00000010"
        |                  ELSE "00000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Vertical_Distance_To_Hydrology < 92.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 492.0 THEN "00000100"
        |                  ELSE "00000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 140.0 THEN "00000110"
        |                  ELSE "00000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Vertical_Distance_To_Hydrology < 47.0 THEN CASE
        |                WHEN T.Vertical_Distance_To_Hydrology < 14.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 42.0 THEN "00001000"
        |                  ELSE "00001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 205.0 THEN "00001010"
        |                  ELSE "00001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_3pm < 142.0 THEN CASE
        |                  WHEN T.Aspect < 91.0 THEN "00001100"
        |                  ELSE "00001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 107.0 THEN "00001110"
        |                  ELSE "00001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |              WHEN T.Vertical_Distance_To_Hydrology < 39.0 THEN CASE
        |                WHEN T.Elevation < 2672.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 85.0 THEN "00010000"
        |                  ELSE "00010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_3pm < 125.0 THEN "00010010"
        |                  ELSE "00010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 256.0 THEN CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 62.0 THEN "00010100"
        |                  ELSE "00010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 69.0 THEN "00010110"
        |                  ELSE "00010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 220.0 THEN CASE
        |                WHEN T.Vertical_Distance_To_Hydrology < 45.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 108.0 THEN "00011000"
        |                  ELSE "00011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_Noon < 248.0 THEN "00011010"
        |                  ELSE "00011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Vertical_Distance_To_Hydrology < 40.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 237.0 THEN "00011100"
        |                  ELSE "00011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 19.0 THEN "00011110"
        |                  ELSE "00011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |            WHEN T.Elevation < 2642.0 THEN CASE
        |              WHEN T.Horizontal_Distance_To_Hydrology < 150.0 THEN CASE
        |                WHEN T.Elevation < 2535.0 THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "00100000"
        |                  ELSE "00100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 225.0 THEN "00100010"
        |                  ELSE "00100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Type_2 < 1.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1226.0 THEN "00100100"
        |                  ELSE "00100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 12.0 THEN "00100110"
        |                  ELSE "00100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 223.0 THEN CASE
        |                WHEN T.Hillshade_3pm < 122.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 203.0 THEN "00101000"
        |                  ELSE "00101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_Noon < 213.0 THEN "00101010"
        |                  ELSE "00101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 223.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 85.0 THEN "00101100"
        |                  ELSE "00101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%Rock%" THEN "00101110"
        |                  ELSE "00101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Slope < 11.0 THEN CASE
        |              WHEN T.Slope < 7.0 THEN CASE
        |                WHEN T.Slope < 5.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 236.0 THEN "00110000"
        |                  ELSE "00110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 6.0 THEN "00110010"
        |                  ELSE "00110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 9.0 THEN CASE
        |                  WHEN T.Slope < 8.0 THEN "00110100"
        |                  ELSE "00110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 10.0 THEN "00110110"
        |                  ELSE "00110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_3pm < 116.0 THEN CASE
        |                WHEN T.Hillshade_3pm < 99.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 600.0 THEN "00111000"
        |                  ELSE "00111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_3pm < 109.0 THEN "00111010"
        |                  ELSE "00111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_Noon < 218.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 211.0 THEN "00111100"
        |                  ELSE "00111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_Noon < 232.0 THEN "00111110"
        |                  ELSE "00111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |      ELSE CASE
        |        WHEN T.Soil_Description LIKE "%Rock%" THEN CASE
        |          WHEN T.Slope < 15.0 THEN CASE
        |            WHEN T.Slope < 10.0 THEN CASE
        |              WHEN T.Slope < 7.0 THEN CASE
        |                WHEN T.Slope < 5.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 237.0 THEN "01000000"
        |                  ELSE "01000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 6.0 THEN "01000010"
        |                  ELSE "01000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_3pm < 142.0 THEN CASE
        |                  WHEN T.Hillshade_3pm < 132.0 THEN "01000100"
        |                  ELSE "01000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 210.0 THEN "01000110"
        |                  ELSE "01000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Slope < 12.0 THEN CASE
        |                WHEN T.Slope < 11.0 THEN CASE
        |                  WHEN T.Hillshade_3pm < 135.0 THEN "01001000"
        |                  ELSE "01001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 228.0 THEN "01001010"
        |                  ELSE "01001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_3pm < 139.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 237.0 THEN "01001100"
        |                  ELSE "01001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 201.0 THEN "01001110"
        |                  ELSE "01001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Horizontal_Distance_To_Fire_Points < 1510.0 THEN CASE
        |              WHEN T.Hillshade_9am < 207.0 THEN CASE
        |                WHEN T.Aspect < 260.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 185.0 THEN "01010000"
        |                  ELSE "01010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 161.0 THEN "01010010"
        |                  ELSE "01010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 993.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%outcrop%" THEN "01010100"
        |                  ELSE "01010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 239.0 THEN "01010110"
        |                  ELSE "01010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_Noon < 212.0 THEN CASE
        |                WHEN T.Hillshade_9am < 226.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 200.0 THEN "01011000"
        |                  ELSE "01011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 76.0 THEN "01011010"
        |                  ELSE "01011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Hydrology < 190.0 THEN CASE
        |                  WHEN T.Slope < 20.0 THEN "01011100"
        |                  ELSE "01011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 371.0 THEN "01011110"
        |                  ELSE "01011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Hillshade_9am < 218.0 THEN CASE
        |            WHEN T.Hillshade_9am < 202.0 THEN CASE
        |              WHEN T.Hillshade_9am < 190.0 THEN CASE
        |                WHEN T.Hillshade_9am < 178.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1678.0 THEN "01100000"
        |                  ELSE "01100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 185.0 THEN "01100010"
        |                  ELSE "01100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 197.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 194.0 THEN "01100100"
        |                  ELSE "01100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 200.0 THEN "01100110"
        |                  ELSE "01100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 211.0 THEN CASE
        |                WHEN T.Hillshade_9am < 207.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 205.0 THEN "01101000"
        |                  ELSE "01101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 209.0 THEN "01101010"
        |                  ELSE "01101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 215.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 213.0 THEN "01101100"
        |                  ELSE "01101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 30.0 THEN "01101110"
        |                  ELSE "01101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Hillshade_9am < 231.0 THEN CASE
        |              WHEN T.Hillshade_9am < 225.0 THEN CASE
        |                WHEN T.Aspect < 46.0 THEN CASE
        |                  WHEN T.Aspect < 40.0 THEN "01110000"
        |                  ELSE "01110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 223.0 THEN "01110010"
        |                  ELSE "01110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 228.0 THEN CASE
        |                  WHEN T.Slope < 9.0 THEN "01110100"
        |                  ELSE "01110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 9.0 THEN "01110110"
        |                  ELSE "01110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Slope < 14.0 THEN CASE
        |                WHEN T.Hillshade_9am < 235.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 233.0 THEN "01111000"
        |                  ELSE "01111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 238.0 THEN "01111010"
        |                  ELSE "01111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 18.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 242.0 THEN "01111100"
        |                  ELSE "01111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 792.0 THEN "01111110"
        |                  ELSE "01111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |    END
        |    ELSE CASE
        |      WHEN T.Hillshade_9am < 217.0 THEN CASE
        |        WHEN T.Aspect < 278.0 THEN CASE
        |          WHEN T.Hillshade_9am < 207.0 THEN CASE
        |            WHEN T.Hillshade_3pm < 183.0 THEN CASE
        |              WHEN T.Hillshade_9am < 203.0 THEN CASE
        |                WHEN T.Hillshade_9am < 197.0 THEN CASE
        |                  WHEN T.Aspect < 7.0 THEN "10000000"
        |                  ELSE "10000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 201.0 THEN "10000010"
        |                  ELSE "10000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 205.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 204.0 THEN "10000100"
        |                  ELSE "10000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 206.0 THEN "10000110"
        |                  ELSE "10000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Aspect < 251.0 THEN CASE
        |                WHEN T.Hillshade_3pm < 193.0 THEN CASE
        |                  WHEN T.Hillshade_3pm < 187.0 THEN "10001000"
        |                  ELSE "10001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 186.0 THEN "10001010"
        |                  ELSE "10001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Aspect < 266.0 THEN CASE
        |                  WHEN T.Aspect < 259.0 THEN "10001100"
        |                  ELSE "10001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 272.0 THEN "10001110"
        |                  ELSE "10001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Hillshade_9am < 213.0 THEN CASE
        |              WHEN T.Hillshade_9am < 210.0 THEN CASE
        |                WHEN T.Slope < 13.0 THEN CASE
        |                  WHEN T.Slope < 10.0 THEN "10010000"
        |                  ELSE "10010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 16.0 THEN "10010010"
        |                  ELSE "10010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 212.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 211.0 THEN "10010100"
        |                  ELSE "10010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 11.0 THEN "10010110"
        |                  ELSE "10010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 215.0 THEN CASE
        |                WHEN T.Hillshade_9am < 214.0 THEN CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "10011000"
        |                  ELSE "10011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1742.0 THEN "10011010"
        |                  ELSE "10011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 216.0 THEN CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "10011100"
        |                  ELSE "10011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "10011110"
        |                  ELSE "10011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Hillshade_9am < 189.0 THEN CASE
        |            WHEN T.Hillshade_9am < 175.0 THEN CASE
        |              WHEN T.Hillshade_9am < 161.0 THEN CASE
        |                WHEN T.Hillshade_9am < 148.0 THEN CASE
        |                  WHEN T.Slope < 29.0 THEN "10100000"
        |                  ELSE "10100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 156.0 THEN "10100010"
        |                  ELSE "10100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 170.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 166.0 THEN "10100100"
        |                  ELSE "10100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 173.0 THEN "10100110"
        |                  ELSE "10100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 183.0 THEN CASE
        |                WHEN T.Hillshade_9am < 179.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 177.0 THEN "10101000"
        |                  ELSE "10101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 181.0 THEN "10101010"
        |                  ELSE "10101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 186.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1694.0 THEN "10101100"
        |                  ELSE "10101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 3197.0 THEN "10101110"
        |                  ELSE "10101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Hillshade_9am < 200.0 THEN CASE
        |              WHEN T.Hillshade_9am < 195.0 THEN CASE
        |                WHEN T.Hillshade_9am < 192.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2382.0 THEN "10110000"
        |                  ELSE "10110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1651.0 THEN "10110010"
        |                  ELSE "10110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 198.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1441.0 THEN "10110100"
        |                  ELSE "10110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 199.0 THEN "10110110"
        |                  ELSE "10110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 206.0 THEN CASE
        |                WHEN T.Hillshade_9am < 203.0 THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "10111000"
        |                  ELSE "10111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 3108.0 THEN "10111010"
        |                  ELSE "10111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 210.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 208.0 THEN "10111100"
        |                  ELSE "10111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 339.0 THEN "10111110"
        |                  ELSE "10111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |      ELSE CASE
        |        WHEN T.Hillshade_9am < 231.0 THEN CASE
        |          WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |            WHEN T.Hillshade_9am < 224.0 THEN CASE
        |              WHEN T.Hillshade_9am < 220.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%Rock%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 2040.0 THEN "11000000"
        |                  ELSE "11000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 36.0 THEN "11000010"
        |                  ELSE "11000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 222.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 221.0 THEN "11000100"
        |                  ELSE "11000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 223.0 THEN "11000110"
        |                  ELSE "11000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                WHEN T.Hillshade_9am < 227.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1734.0 THEN "11001000"
        |                  ELSE "11001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 229.0 THEN "11001010"
        |                  ELSE "11001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%family,%" THEN CASE
        |                  WHEN T.Hillshade_9am < 227.0 THEN "11001100"
        |                  ELSE "11001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1591.0 THEN "11001110"
        |                  ELSE "11001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Hillshade_9am < 225.0 THEN CASE
        |              WHEN T.Hillshade_9am < 221.0 THEN CASE
        |                WHEN T.Hillshade_9am < 219.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Legault%" THEN "11010000"
        |                  ELSE "11010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 220.0 THEN "11010010"
        |                  ELSE "11010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 223.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 222.0 THEN "11010100"
        |                  ELSE "11010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 224.0 THEN "11010110"
        |                  ELSE "11010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 228.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 4032.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%family%" THEN "11011000"
        |                  ELSE "11011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%Legault%" THEN "11011010"
        |                  ELSE "11011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 3803.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1972.0 THEN "11011100"
        |                  ELSE "11011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 5045.0 THEN "11011110"
        |                  ELSE "11011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Hillshade_9am < 238.0 THEN CASE
        |            WHEN T.Hillshade_9am < 234.0 THEN CASE
        |              WHEN T.Horizontal_Distance_To_Fire_Points < 2272.0 THEN CASE
        |                WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Catamount%" THEN "11100000"
        |                  ELSE "11100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%Legault%" THEN "11100010"
        |                  ELSE "11100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 3019.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%family%" THEN "11100100"
        |                  ELSE "11100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 3569.0 THEN "11100110"
        |                  ELSE "11100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 236.0 THEN CASE
        |                WHEN T.Hillshade_9am < 235.0 THEN CASE
        |                  WHEN T.Slope < 11.0 THEN "11101000"
        |                  ELSE "11101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%family%" THEN "11101010"
        |                  ELSE "11101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 237.0 THEN CASE
        |                  WHEN T.Slope < 12.0 THEN "11101100"
        |                  ELSE "11101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 13.0 THEN "11101110"
        |                  ELSE "11101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Hillshade_9am < 243.0 THEN CASE
        |              WHEN T.Hillshade_9am < 240.0 THEN CASE
        |                WHEN T.Hillshade_9am < 239.0 THEN CASE
        |                  WHEN T.Slope < 13.0 THEN "11110000"
        |                  ELSE "11110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 13.0 THEN "11110010"
        |                  ELSE "11110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |                  WHEN T.Soil_Description LIKE "%Catamount%" THEN "11110100"
        |                  ELSE "11110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_29 < 1.0 THEN "11110110"
        |                  ELSE "11110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Slope < 19.0 THEN CASE
        |                WHEN T.Hillshade_9am < 245.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 244.0 THEN "11111000"
        |                  ELSE "11111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_3pm < 98.0 THEN "11111010"
        |                  ELSE "11111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 23.0 THEN CASE
        |                  WHEN T.Slope < 21.0 THEN "11111100"
        |                  ELSE "11111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%families%" THEN "11111110"
        |                  ELSE "11111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |    END
        |  END""".stripMargin,
    "label_c" ->
      """CASE
        |    WHEN T.Soil_Description LIKE "%Leighcan%" THEN CASE
        |      WHEN T.Hillshade_9am < 214.0 THEN CASE
        |        WHEN T.Hillshade_9am < 195.0 THEN CASE
        |          WHEN T.Elevation < 3099.0 THEN CASE
        |            WHEN T.Soil_Description LIKE "%family%" THEN CASE
        |              WHEN T.Soil_Description LIKE "%stony.%" THEN CASE
        |                WHEN T.Hillshade_9am < 179.0 THEN CASE
        |                  WHEN T.Hillshade_3pm < 195.0 THEN "00000000"
        |                  ELSE "00000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 188.0 THEN "00000010"
        |                  ELSE "00000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Type_23 < 1.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 184.0 THEN "00000100"
        |                  ELSE "00000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 15.0 THEN "00000110"
        |                  ELSE "00000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 175.0 THEN CASE
        |                WHEN T.Hillshade_9am < 155.0 THEN CASE
        |                  WHEN T.Slope < 29.0 THEN "00001000"
        |                  ELSE "00001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1362.0 THEN "00001010"
        |                  ELSE "00001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 186.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 182.0 THEN "00001100"
        |                  ELSE "00001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 191.0 THEN "00001110"
        |                  ELSE "00001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Horizontal_Distance_To_Fire_Points < 1579.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                WHEN T.Hillshade_3pm < 191.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 752.0 THEN "00010000"
        |                  ELSE "00010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 276.0 THEN "00010010"
        |                  ELSE "00010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%till%" THEN CASE
        |                  WHEN T.Soil_Type_22 < 1.0 THEN "00010100"
        |                  ELSE "00010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 3443.0 THEN "00010110"
        |                  ELSE "00010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |                WHEN T.Hillshade_9am < 182.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2292.0 THEN "00011000"
        |                  ELSE "00011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 189.0 THEN "00011010"
        |                  ELSE "00011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%family,%" THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2098.0 THEN "00011100"
        |                  ELSE "00011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Wilderness_Area_1 < 1.0 THEN "00011110"
        |                  ELSE "00011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Horizontal_Distance_To_Fire_Points < 1426.0 THEN CASE
        |            WHEN T.Elevation < 3131.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%family,%" THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 1418.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 207.0 THEN "00100000"
        |                  ELSE "00100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 283.0 THEN "00100010"
        |                  ELSE "00100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 205.0 THEN CASE
        |                  WHEN T.Aspect < 238.0 THEN "00100100"
        |                  ELSE "00100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 969.0 THEN "00100110"
        |                  ELSE "00100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Fire_Points < 841.0 THEN CASE
        |                WHEN T.Hillshade_3pm < 171.0 THEN CASE
        |                  WHEN T.Aspect < 291.0 THEN "00101000"
        |                  ELSE "00101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 558.0 THEN "00101010"
        |                  ELSE "00101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_3pm < 168.0 THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "00101100"
        |                  ELSE "00101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%outcrop%" THEN "00101110"
        |                  ELSE "00101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Hillshade_9am < 206.0 THEN CASE
        |              WHEN T.Hillshade_9am < 201.0 THEN CASE
        |                WHEN T.Hillshade_9am < 198.0 THEN CASE
        |                  WHEN T.Aspect < 286.0 THEN "00110000"
        |                  ELSE "00110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2190.0 THEN "00110010"
        |                  ELSE "00110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 203.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 202.0 THEN "00110100"
        |                  ELSE "00110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2227.0 THEN "00110110"
        |                  ELSE "00110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 210.0 THEN CASE
        |                WHEN T.Hillshade_9am < 208.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 207.0 THEN "00111000"
        |                  ELSE "00111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 209.0 THEN "00111010"
        |                  ELSE "00111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 212.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 211.0 THEN "00111100"
        |                  ELSE "00111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 213.0 THEN "00111110"
        |                  ELSE "00111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |      ELSE CASE
        |        WHEN T.Hillshade_9am < 227.0 THEN CASE
        |          WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |            WHEN T.Hillshade_9am < 221.0 THEN CASE
        |              WHEN T.Hillshade_9am < 218.0 THEN CASE
        |                WHEN T.Elevation < 3096.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1515.0 THEN "01000000"
        |                  ELSE "01000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 216.0 THEN "01000010"
        |                  ELSE "01000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1624.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 916.0 THEN "01000100"
        |                  ELSE "01000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_32 < 1.0 THEN "01000110"
        |                  ELSE "01000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 224.0 THEN CASE
        |                WHEN T.Slope < 11.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1879.0 THEN "01001000"
        |                  ELSE "01001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1687.0 THEN "01001010"
        |                  ELSE "01001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 12.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1725.0 THEN "01001100"
        |                  ELSE "01001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 17.0 THEN "01001110"
        |                  ELSE "01001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Soil_Type_23 < 1.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%till%" THEN CASE
        |                WHEN T.Hillshade_9am < 222.0 THEN CASE
        |                  WHEN T.Aspect < 40.0 THEN "01010000"
        |                  ELSE "01010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 225.0 THEN "01010010"
        |                  ELSE "01010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Moran%" THEN CASE
        |                  WHEN T.Wilderness_Area_3 < 1.0 THEN "01010100"
        |                  ELSE "01010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 43.0 THEN "01010110"
        |                  ELSE "01010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 221.0 THEN CASE
        |                WHEN T.Hillshade_9am < 217.0 THEN CASE
        |                  WHEN T.Aspect < 30.0 THEN "01011000"
        |                  ELSE "01011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 219.0 THEN "01011010"
        |                  ELSE "01011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 224.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1826.0 THEN "01011100"
        |                  ELSE "01011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 7.0 THEN "01011110"
        |                  ELSE "01011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Horizontal_Distance_To_Roadways < 1717.0 THEN CASE
        |            WHEN T.Hillshade_9am < 237.0 THEN CASE
        |              WHEN T.Hillshade_9am < 232.0 THEN CASE
        |                WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 229.0 THEN "01100000"
        |                  ELSE "01100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1222.0 THEN "01100010"
        |                  ELSE "01100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 235.0 THEN "01100100"
        |                  ELSE "01100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 234.0 THEN "01100110"
        |                  ELSE "01100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Fire_Points < 1717.0 THEN CASE
        |                WHEN T.Soil_Description LIKE "%Rock%" THEN CASE
        |                  WHEN T.Hillshade_9am < 243.0 THEN "01101000"
        |                  ELSE "01101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%Moran%" THEN "01101010"
        |                  ELSE "01101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 1045.0 THEN CASE
        |                  WHEN T.Slope < 16.0 THEN "01101100"
        |                  ELSE "01101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%Catamount%" THEN "01101110"
        |                  ELSE "01101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Elevation < 3199.0 THEN CASE
        |              WHEN T.Hillshade_9am < 234.0 THEN CASE
        |                WHEN T.Hillshade_9am < 230.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1983.0 THEN "01110000"
        |                  ELSE "01110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 232.0 THEN "01110010"
        |                  ELSE "01110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 16.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 238.0 THEN "01110100"
        |                  ELSE "01110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 20.0 THEN "01110110"
        |                  ELSE "01110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Description LIKE "%Moran%" THEN CASE
        |                WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |                  WHEN T.Aspect < 94.0 THEN "01111000"
        |                  ELSE "01111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 104.0 THEN "01111010"
        |                  ELSE "01111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 236.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1788.0 THEN "01111100"
        |                  ELSE "01111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2656.0 THEN "01111110"
        |                  ELSE "01111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |    END
        |    ELSE CASE
        |      WHEN T.Wilderness_Area_1 < 1.0 THEN CASE
        |        WHEN T.Elevation < 2730.0 THEN CASE
        |          WHEN T.Wilderness_Area_3 < 1.0 THEN CASE
        |            WHEN T.Hillshade_3pm < 154.0 THEN CASE
        |              WHEN T.Hillshade_9am < 226.0 THEN CASE
        |                WHEN T.Vertical_Distance_To_Hydrology < 50.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 85.0 THEN "10000000"
        |                  ELSE "10000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 93.0 THEN "10000010"
        |                  ELSE "10000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%families%" THEN CASE
        |                  WHEN T.Elevation < 2384.0 THEN "10000100"
        |                  ELSE "10000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Description LIKE "%family%" THEN "10000110"
        |                  ELSE "10000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 730.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 408.0 THEN CASE
        |                  WHEN T.Elevation < 2211.0 THEN "10001000"
        |                  ELSE "10001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_3pm < 194.0 THEN "10001010"
        |                  ELSE "10001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 2346.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 218.0 THEN "10001100"
        |                  ELSE "10001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 315.0 THEN "10001110"
        |                  ELSE "10001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Soil_Description LIKE "%Ratake%" THEN CASE
        |              WHEN T.Vertical_Distance_To_Hydrology < 34.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1101.0 THEN CASE
        |                  WHEN T.Vertical_Distance_To_Hydrology < 10.0 THEN "10010000"
        |                  ELSE "10010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Hydrology < 108.0 THEN "10010010"
        |                  ELSE "10010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_3pm < 141.0 THEN CASE
        |                  WHEN T.Aspect < 122.0 THEN "10010100"
        |                  ELSE "10010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 204.0 THEN "10010110"
        |                  ELSE "10010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Aspect < 83.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1237.0 THEN CASE
        |                  WHEN T.Elevation < 2628.0 THEN "10011000"
        |                  ELSE "10011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1710.0 THEN "10011010"
        |                  ELSE "10011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Aspect < 211.0 THEN CASE
        |                  WHEN T.Elevation < 2627.0 THEN "10011100"
        |                  ELSE "10011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1294.0 THEN "10011110"
        |                  ELSE "10011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Soil_Description LIKE "%Catamount%" THEN CASE
        |            WHEN T.Hillshade_9am < 224.0 THEN CASE
        |              WHEN T.Aspect < 225.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1321.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 209.0 THEN "10100000"
        |                  ELSE "10100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 45.0 THEN "10100010"
        |                  ELSE "10100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1361.0 THEN CASE
        |                  WHEN T.Slope < 20.0 THEN "10100100"
        |                  ELSE "10100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 284.0 THEN "10100110"
        |                  ELSE "10100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Soil_Type_13 < 1.0 THEN CASE
        |                WHEN T.Aspect < 99.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 233.0 THEN "10101000"
        |                  ELSE "10101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 238.0 THEN "10101010"
        |                  ELSE "10101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 21.0 THEN CASE
        |                  WHEN T.Slope < 16.0 THEN "10101100"
        |                  ELSE "10101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1413.0 THEN "10101110"
        |                  ELSE "10101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Elevation < 2934.0 THEN CASE
        |              WHEN T.Soil_Description LIKE "%Ratake%" THEN CASE
        |                WHEN T.Hillshade_Noon < 247.0 THEN CASE
        |                  WHEN T.Elevation < 2805.0 THEN "10110000"
        |                  ELSE "10110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1350.0 THEN "10110010"
        |                  ELSE "10110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Soil_Description LIKE "%Typic%" THEN "1011010"
        |                ELSE CASE
        |                  WHEN T.Soil_Type_17 < 1.0 THEN "10110110"
        |                  ELSE "10110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Elevation < 3389.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 2221.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1357.0 THEN "10111000"
        |                  ELSE "10111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Soil_Type_34 < 1.0 THEN "10111010"
        |                  ELSE "10111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Elevation < 3475.0 THEN CASE
        |                  WHEN T.label LIKE "%11111111%" THEN "10111100"
        |                  ELSE "10111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 115.0 THEN "10111110"
        |                  ELSE "10111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |      ELSE CASE
        |        WHEN T.Hillshade_9am < 223.0 THEN CASE
        |          WHEN T.Slope < 11.0 THEN CASE
        |            WHEN T.Slope < 7.0 THEN CASE
        |              WHEN T.Hillshade_9am < 217.0 THEN CASE
        |                WHEN T.Slope < 5.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 214.0 THEN "11000000"
        |                  ELSE "11000001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 285.0 THEN "11000010"
        |                  ELSE "11000011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 4.0 THEN CASE
        |                  WHEN T.Hillshade_Noon < 236.0 THEN "11000100"
        |                  ELSE "11000101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Aspect < 45.0 THEN "11000110"
        |                  ELSE "11000111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Slope < 9.0 THEN CASE
        |                WHEN T.Slope < 8.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 214.0 THEN "11001000"
        |                  ELSE "11001001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 2291.0 THEN "11001010"
        |                  ELSE "11001011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 10.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 209.0 THEN "11001100"
        |                  ELSE "11001101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 207.0 THEN "11001110"
        |                  ELSE "11001111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Aspect < 257.0 THEN CASE
        |              WHEN T.Hillshade_9am < 211.0 THEN CASE
        |                WHEN T.Slope < 16.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 204.0 THEN "11010000"
        |                  ELSE "11010001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 20.0 THEN "11010010"
        |                  ELSE "11010011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 217.0 THEN CASE
        |                  WHEN T.Elevation < 2978.0 THEN "11010100"
        |                  ELSE "11010101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Elevation < 2943.0 THEN "11010110"
        |                  ELSE "11010111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Hillshade_9am < 183.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 2012.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1396.0 THEN "11011000"
        |                  ELSE "11011001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 166.0 THEN "11011010"
        |                  ELSE "11011011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Hillshade_9am < 190.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 187.0 THEN "11011100"
        |                  ELSE "11011101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 195.0 THEN "11011110"
        |                  ELSE "11011111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |        ELSE CASE
        |          WHEN T.Horizontal_Distance_To_Fire_Points < 2353.0 THEN CASE
        |            WHEN T.Elevation < 2974.0 THEN CASE
        |              WHEN T.Slope < 12.0 THEN CASE
        |                WHEN T.Aspect < 97.0 THEN CASE
        |                  WHEN T.Aspect < 72.0 THEN "11100000"
        |                  ELSE "11100001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Slope < 8.0 THEN "11100010"
        |                  ELSE "11100011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 702.0 THEN CASE
        |                  WHEN T.Elevation < 2839.0 THEN "11100100"
        |                  ELSE "11100101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_9am < 241.0 THEN "11100110"
        |                  ELSE "11100111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 3861.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 1473.0 THEN CASE
        |                  WHEN T.Aspect < 93.0 THEN "11101000"
        |                  ELSE "11101001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1766.0 THEN "11101010"
        |                  ELSE "11101011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 4848.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 4419.0 THEN "11101100"
        |                  ELSE "11101101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Fire_Points < 1296.0 THEN "11101110"
        |                  ELSE "11101111"
        |                END
        |              END
        |            END
        |          END
        |          ELSE CASE
        |            WHEN T.Elevation < 2968.0 THEN CASE
        |              WHEN T.Hillshade_9am < 234.0 THEN CASE
        |                WHEN T.Hillshade_9am < 229.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 226.0 THEN "11110000"
        |                  ELSE "11110001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_Noon < 230.0 THEN "11110010"
        |                  ELSE "11110011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Slope < 15.0 THEN CASE
        |                  WHEN T.Slope < 12.0 THEN "11110100"
        |                  ELSE "11110101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Hillshade_3pm < 95.0 THEN "11110110"
        |                  ELSE "11110111"
        |                END
        |              END
        |            END
        |            ELSE CASE
        |              WHEN T.Horizontal_Distance_To_Roadways < 4222.0 THEN CASE
        |                WHEN T.Horizontal_Distance_To_Fire_Points < 2958.0 THEN CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 1777.0 THEN "11111000"
        |                  ELSE "11111001"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 3147.0 THEN "11111010"
        |                  ELSE "11111011"
        |                END
        |              END
        |              ELSE CASE
        |                WHEN T.Horizontal_Distance_To_Roadways < 5208.0 THEN CASE
        |                  WHEN T.Hillshade_9am < 233.0 THEN "11111100"
        |                  ELSE "11111101"
        |                END
        |                ELSE CASE
        |                  WHEN T.Horizontal_Distance_To_Roadways < 5958.0 THEN "11111110"
        |                  ELSE "11111111"
        |                END
        |              END
        |            END
        |          END
        |        END
        |      END
        |    END
        |  END""".stripMargin
  ) - "label_c"

  def predictiveMatrix(sourceCol: String, sourceData: DataFrame = sourceDataFrame, targetCol: String = targetCol) = {
    val totals = sourceData.select(sourceCol).groupBy(sourceCol).agg(count(lit(1)).as("count")).cache()
    val relabeledOut = sourceData.withColumnRenamed(targetCol, "raw_" + targetCol)
      .withColumn(targetCol, concat(lit(targetCol + "_"), col("raw_" + targetCol)))
      .drop("raw_" + targetCol)
      .cache()
    val joined = relabeledOut
      .join(totals, relabeledOut(sourceCol) === (totals(sourceCol)))
      .select(relabeledOut(sourceCol), relabeledOut(targetCol), totals("count"))
      .cache()
    joined
      .groupBy(joined(sourceCol)).pivot(targetCol)
      .agg((sum(lit(1.0).divide(joined("count")))))
      .sort(relabeledOut(sourceCol))
      .cache()
  }
}
