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

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.repl.{SparkRepl, SparkSessionProvider}
import org.apache.spark.sql.SaveMode

object CovType_Unpack_Local extends CovType_Unpack with LocalRunner with NotebookRunner {
  override def http_port = 1081
}

object CovType_Unpack_EC2 extends CovType_Unpack with EC2Runner with AWSNotebookRunner {
  override def s3bucket: String = super.s3bucket

  override def nodeSettings: EC2NodeSettings = EC2NodeSettings.T2_L
}

abstract class CovType_Unpack extends SparkRepl with Logging with SparkSessionProvider {

  val destination = "s3a://simiacryptus/data/covtype/"

  override def init(): Unit = {
    Thread.sleep(30000)
    val frame = CovType.dataframe(spark)
    //frame.createOrReplaceTempView("covtype")
    frame.write.mode(SaveMode.Overwrite).parquet(destination)
    frame.sparkSession.sqlContext.read.parquet(destination).createOrReplaceTempView("covtype")
  }

  override val defaultCmd: String =
    """%sql
      |SELECT COUNT(*) AS count, Cover_Type FROM covtype GROUP BY Cover_Type
    """.stripMargin
}
