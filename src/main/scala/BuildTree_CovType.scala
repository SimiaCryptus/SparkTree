import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook._
import com.simiacryptus.util.io.NotebookOutput
import org.apache.spark.sql.types._

object BuildTree_CovType_Local extends BuildTree_CovType with LocalRunner with NotebookRunner

object BuildTree_CovType_Embedded extends BuildTree_CovType with EmbeddedSparkRunner with NotebookRunner {

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "2g"

}

object BuildTree_CovType_EC2 extends BuildTree_CovType with EC2SparkRunner with AWSNotebookRunner {

  override def numberOfWorkerNodes: Int = 2

  override def numberOfWorkersPerNode: Int = 8

  override def driverMemory: String = "2g"

  override def workerMemory: String = "2g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.T2_L

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.T2_XL

}


abstract class BuildTree_CovType extends BuildTree {
  override val dataSource: String = "s3a://simiacryptus/data/covtype/"
  val target = Array("Cover_Type")

  val sourceTableName: String = """covtype"""

  override def ruleBlacklist = target

  val supervision: String = "unsupervised"

  def entropySpec: Map[String, Double] = sourceDataFrame.schema
    .filterNot(_.name.startsWith("Soil_Type"))
    .map(field => field.dataType match {
      case StringType =>
        val avgLength = sourceDataFrame.select(sourceDataFrame.col(field.name)).rdd.map(_.getAs[String](0).length).mean
        field.name -> 1.0 / avgLength
      case _ => field.name -> 1.0
    })
    .filter(tuple => supervision match {
      case "unsupervised" =>
        !ruleBlacklist.contains(tuple._1)
      case "semi-supervised" =>
        true
      case "supervised" =>
        !ruleBlacklist.contains(tuple._1)
    })
    .toMap

  def statsSpec: List[String] = sourceDataFrame.schema.map(_.name).toList

  override def validationColumns = target

}
