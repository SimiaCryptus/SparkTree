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

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.util.zip.GZIPInputStream

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.simiacryptus.aws.{EC2Util, S3Util}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CovType {
  val colNames: List[(String, DataType)] = List(
    /* 0 */ "Elevation" -> IntegerType,
    "Aspect" -> IntegerType,
    "Slope" -> IntegerType,
    "Horizontal_Distance_To_Hydrology" -> IntegerType,
    "Vertical_Distance_To_Hydrology" -> IntegerType,
    "Horizontal_Distance_To_Roadways" -> IntegerType,
    "Hillshade_9am" -> IntegerType,
    "Hillshade_Noon" -> IntegerType,
    "Hillshade_3pm" -> IntegerType,
    "Horizontal_Distance_To_Fire_Points" -> IntegerType,
    /* 10 */ "Wilderness_Area_1" -> IntegerType,
    "Wilderness_Area_2" -> IntegerType,
    "Wilderness_Area_3" -> IntegerType,
    "Wilderness_Area_4" -> IntegerType,
    /* 14 */ "Soil_Type_1" -> IntegerType,
    "Soil_Type_2" -> IntegerType,
    "Soil_Type_3" -> IntegerType,
    "Soil_Type_4" -> IntegerType,
    "Soil_Type_5" -> IntegerType,
    "Soil_Type_6" -> IntegerType,
    "Soil_Type_7" -> IntegerType,
    "Soil_Type_8" -> IntegerType,
    "Soil_Type_9" -> IntegerType,
    "Soil_Type_10" -> IntegerType,
    "Soil_Type_11" -> IntegerType,
    "Soil_Type_12" -> IntegerType,
    "Soil_Type_13" -> IntegerType,
    "Soil_Type_14" -> IntegerType,
    "Soil_Type_15" -> IntegerType,
    "Soil_Type_16" -> IntegerType,
    "Soil_Type_17" -> IntegerType,
    "Soil_Type_18" -> IntegerType,
    "Soil_Type_19" -> IntegerType,
    "Soil_Type_20" -> IntegerType,
    "Soil_Type_21" -> IntegerType,
    "Soil_Type_22" -> IntegerType,
    "Soil_Type_23" -> IntegerType,
    "Soil_Type_24" -> IntegerType,
    "Soil_Type_25" -> IntegerType,
    "Soil_Type_26" -> IntegerType,
    "Soil_Type_27" -> IntegerType,
    "Soil_Type_28" -> IntegerType,
    "Soil_Type_29" -> IntegerType,
    "Soil_Type_30" -> IntegerType,
    "Soil_Type_31" -> IntegerType,
    "Soil_Type_32" -> IntegerType,
    "Soil_Type_33" -> IntegerType,
    "Soil_Type_34" -> IntegerType,
    "Soil_Type_35" -> IntegerType,
    "Soil_Type_36" -> IntegerType,
    "Soil_Type_37" -> IntegerType,
    "Soil_Type_38" -> IntegerType,
    "Soil_Type_39" -> IntegerType,
    "Soil_Type_40" -> IntegerType,
    /* 54 */ "Cover_Type" -> IntegerType,
    /* 55 */ "Soil_Description" -> StringType
  )
  val soilTypes =
    """
      Cathedral family - Rock outcrop complex, extremely stony.
      Vanet - Ratake families complex, very stony.
      Haploborolis - Rock outcrop complex, rubbly.
      Ratake family - Rock outcrop complex, rubbly.
      Vanet family - Rock outcrop complex complex, rubbly.
      Vanet - Wetmore families - Rock outcrop complex, stony.
      Gothic family.
      Supervisor - Limber families complex.
      Troutville family, very stony.
      Bullwark - Catamount families - Rock outcrop complex, rubbly.
      Bullwark - Catamount families - Rock land complex, rubbly.
      Legault family - Rock land complex, stony.
      Catamount family - Rock land - Bullwark family complex, rubbly.
      Pachic Argiborolis - Aquolis complex.
      unspecified in the USFS Soil and ELU Survey.
      Cryaquolis - Cryoborolis complex.
      Gateview family - Cryaquolis complex.
      Rogert family, very stony.
      Typic Cryaquolis - Borohemists complex.
      Typic Cryaquepts - Typic Cryaquolls complex.
      Typic Cryaquolls - Leighcan family, till substratum complex.
      Leighcan family, till substratum, extremely bouldery.
      Leighcan family, till substratum - Typic Cryaquolls complex.
      Leighcan family, extremely stony.
      Leighcan family, warm, extremely stony.
      Granile - Catamount families complex, very stony.
      Leighcan family, warm - Rock outcrop complex, extremely stony.
      Leighcan family - Rock outcrop complex, extremely stony.
      Como - Legault families complex, extremely stony.
      Como family - Rock land - Legault family complex, extremely stony.
      Leighcan - Catamount families complex, extremely stony.
      Catamount family - Rock outcrop - Leighcan family complex, extremely stony.
      Leighcan - Catamount families - Rock outcrop complex, extremely stony.
      Cryorthents - Rock land complex, extremely stony.
      Cryumbrepts - Rock outcrop - Cryaquepts complex.
      Bross family - Rock land - Cryumbrepts complex, extremely stony.
      Rock outcrop - Cryumbrepts - Cryorthents complex, extremely stony.
      Leighcan - Moran families - Cryaquolls complex, extremely stony.
      Moran family - Cryorthents - Leighcan family complex, extremely stony.
      Moran family - Cryorthents - Rock land complex, extremely stony.
    """.stripMargin.split("\n").map(_.trim).filterNot(_.isEmpty).toArray

  def dataframe(session: SparkSession): DataFrame = {
    val reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(S3Util.cache(
      AmazonS3ClientBuilder.standard.withRegion(EC2Util.REGION).build,
      new URI("s3://simiacryptus/covtype.data.gz"),
      new URI("https://archive.ics.uci.edu/ml/machine-learning-databases/covtype/covtype.data.gz")
    ))))
    val rows = Stream.continually(reader.readLine()).takeWhile(null != _)
      .map(str => {
        val data = str.split(",").map(_.toInt)
        val soiltype = (14 until 54).filter(data(_) == 1).map(_ - 14).map(soilTypes(_))
        require(soiltype.size == 1)
        data ++ Array(soiltype.head)
      }).map(Row(_: _*))
    val rowRdd = session.sparkContext.parallelize(rows).cache()
    require(rowRdd.count() > 0)
    reader.close()
    session.sqlContext.createDataFrame(
      rowRdd,
      StructType(
        colNames.map(e => StructField(e._1,
          e._2,
          false)).toList
      )
    )
  }
}
