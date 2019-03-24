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
import org.apache.spark.sql.types._

abstract class CovType_BuildTree extends TreeBuilder {

  override val dataSources = Map(
    "s3a://simiacryptus/data/covtype/" -> "src_covtype"
  )
  val target = Array("Cover_Type")
  val sourceTableName: String = "covtype"
  val supervision: String = "supervised"

  def entropySpec(schema: StructType = sourceDataFrame.schema): Map[String, Double] = {
    schema
      .filterNot(_.name.startsWith("Soil_Type"))
      .map(field => field.dataType match {
        //      case StringType =>
        //        val avgLength = sourceDataFrame.select(sourceDataFrame.col(field.name)).rdd.map(_.getAs[String](0).length).mean
        //        field.name -> 1.0 / avgLength
        case _ => field.name -> 1.0
      })
      .filter(tuple => supervision match {
        case "unsupervised" =>
          !ruleBlacklist.contains(tuple._1)
        case "semi-supervised" =>
          true
        case "supervised" =>
          ruleBlacklist.contains(tuple._1)
      })
      .toMap
  }

  override def ruleBlacklist = target

  def statsSpec(schema: StructType = sourceDataFrame.schema): List[String] = schema.map(_.name).toList

  override def validationColumns = target

}

object CovType_BuildTree_Local extends CovType_BuildTree with LocalRunner[Object] with NotebookRunner[Object]

object CovType_BuildTree_Embedded extends CovType_BuildTree with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2
  override val numberOfWorkersPerNode: Int = 2
  override val workerMemory: String = "2g"

  override def hiveRoot: Option[String] = super.hiveRoot

}

object CovType_BuildTree_EC2 extends CovType_BuildTree with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  override val s3bucket: String = envTuple._2
  override val numberOfWorkerNodes: Int = 1
  override val numberOfWorkersPerNode: Int = 1
  override val workerCores: Int = 8
  override val driverMemory: String = "14g"
  override val workerMemory: String = "14g"

  override def hiveRoot: Option[String] = super.hiveRoot

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

}
