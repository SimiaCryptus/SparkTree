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

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.util.LocalRunner

abstract class CovType_Forest extends MultivariatePredictor {

  override val dataSources = Map(
    "s3a://simiacryptus/data/covtype/" -> "src_covtype"
  )
  val target = Array("Cover_Type")
  val sourceTableName: String = "covtype"

}

object CovType_Forest_Local extends CovType_Forest with LocalRunner[Object] with NotebookRunner[Object]

object CovType_Forest_Embedded extends CovType_Forest with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2
  override val numberOfWorkersPerNode: Int = 2
  override val workerMemory: String = "2g"

  override def hiveRoot: Option[String] = super.hiveRoot

}

object CovType_Forest_EC2 extends CovType_Forest with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  override val s3bucket: String = envTuple._2
  override val numberOfWorkerNodes: Int = 2
  override val numberOfWorkersPerNode: Int = 7
  override val driverMemory: String = "14g"
  override val workerMemory: String = "2g"

  override def hiveRoot: Option[String] = super.hiveRoot

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

}
