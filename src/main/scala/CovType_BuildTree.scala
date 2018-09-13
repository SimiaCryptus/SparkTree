import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook._
import org.apache.spark.sql.types._

object CovType_BuildTree_Local extends CovType_BuildTree with LocalRunner with NotebookRunner

object CovType_BuildTree_Embedded extends CovType_BuildTree with EmbeddedSparkRunner with NotebookRunner {

  override def s3bucket: String = super.s3bucket

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "2g"

}

object CovType_BuildTree_EC2 extends CovType_BuildTree with EC2SparkRunner with AWSNotebookRunner {

  override def s3bucket: String = super.s3bucket

  override def numberOfWorkerNodes: Int = 2

  override def numberOfWorkersPerNode: Int = 7

  override def driverMemory: String = "14g"

  override def workerMemory: String = "2g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

}


abstract class CovType_BuildTree extends TreeBuilder {
  override val dataSources = Map(
    "s3a://simiacryptus/data/covtype/" -> "src_covtype"
  )
  val target = Array("Cover_Type")

  val sourceTableName: String = "covtype"

  override def ruleBlacklist = target

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

  def statsSpec(schema: StructType = sourceDataFrame.schema): List[String] = schema.map(_.name).toList

  override def validationColumns = target

}
