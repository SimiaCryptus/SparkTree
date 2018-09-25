import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.util.LocalRunner
import org.apache.spark.sql.types._

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

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "2g"

}

object CovType_Forest_EC2 extends CovType_Forest with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2

  override def numberOfWorkerNodes: Int = 2

  override def numberOfWorkersPerNode: Int = 7

  override def driverMemory: String = "14g"

  override def workerMemory: String = "2g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

}
