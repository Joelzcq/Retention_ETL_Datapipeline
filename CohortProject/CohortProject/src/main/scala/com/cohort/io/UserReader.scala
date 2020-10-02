package com.cohort.io

import com.cohort.conf.CohortConf
import com.cohort.util.Utils
import com.cohort.process.UserProcess.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait UserReader extends Logging{

  def readUserInfo(conf: CohortConf, spark: SparkSession, date: String): DataFrame = {
    val inputPath = Utils.pathGenerator(conf.uniqueUserPath(), conf.datePrefix(), date)

    logInfo("reading from %s".format(inputPath))

    val inputUniqueUsersDf: DataFrame = try { //reading unique user list
      Some(spark.read.json(inputPath)).get
    } catch {
      case e: Exception => spark.emptyDataFrame.withColumn("user_id", lit(null: StringType))
        .withColumn("first_timestamp", lit(null: StringType))
    }

    inputUniqueUsersDf
  }

}
