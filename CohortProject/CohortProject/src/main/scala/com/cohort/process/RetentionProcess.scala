package com.cohort.process

import com.cohort.conf.CohortConf
import com.cohort.io._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object RetentionProcess extends Logging with UserReader with BikeShareTripReader {

  def main(args: Array[String]): Unit = {
    val conf                  = new CohortConf(args)
    val spark = SparkSession
      .builder()
      .appName("Bike-share")
      .getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    retentionPrep(spark, conf)
  }

  def retentionPrep(spark: SparkSession, conf: CohortConf): Unit = {
    val bikeShareDf = readBikeShareTrip(conf, spark)
    bikeShareDf.printSchema()
    val userDf = readUserInfo(conf, spark, conf.processDate())
    userDf.printSchema()
    val dayAgoBikeShareDf = readDayAgoBikeShareTrip(conf, spark)
    dayAgoBikeShareDf.printSchema()

    val joinedBikeSharedDf = bikeShareDf.join(userDf,
      bikeShareDf.col("user_id") === userDf.col("user_id"), "left")
      .drop(bikeShareDf.col("user_id"))

    val bikeUserAgeDays = joinedBikeSharedDf
      .withColumn("user_age_days",
        datediff(to_date(col("start_timestamp")), to_date(col("first_timestamp"))))

    val bikeFilteredDf : DataFrame = conf.dayAgo() match {
      case 1 => bikeUserAgeDays.filter((col("user_age_days") === 1))
      case 3 => bikeUserAgeDays.filter((col("user_age_days") === 3))
      case 7 => bikeUserAgeDays.filter((col("user_age_days") === 7))
      case _ => throw new Exception("input date is invalid")
    }

    val bikeFilteredAgoDf = bikeFilteredDf.select("user_id", "user_age_days").distinct()

    val aggPrepDf = dayAgoBikeShareDf
      .join(bikeFilteredAgoDf, dayAgoBikeShareDf.col("user_id") === bikeFilteredAgoDf.col("user_id"), "left")
      .drop(bikeFilteredAgoDf.col("user_id"))

    val groupbyFields = BikeShareProcess.fields :+ BikeShareProcess.avgDurationSec

    if(!aggPrepDf.columns.contains("user_age_days") || aggPrepDf.count() == 0){
      logInfo("didn't find anyone fit into %s day ago".format(conf.dayAgo()))
      val aggPrepDfWithageDays = aggPrepDf.withColumn("user_age_days", lit(0))
      retentionAndSave(aggPrepDfWithageDays, conf)
    } else {
      retentionAndSave(aggPrepDf, conf)
    }
  }

  def retentionAndSave(df: DataFrame, conf: CohortConf): Unit = {
    val groupbyFields = BikeShareProcess.fields :+ BikeShareProcess.avgDurationSec :+ "user_age_days"

    val bikeUserAggDf = df.groupBy(groupbyFields.map(col):_*)
      .agg(max(when(df.col("user_age_days") === 1, 1).otherwise(0)).alias("retention_1"),
        max(when(df.col("user_age_days") === 3, 1).otherwise(0)).alias("retention_3"),
        max(when(df.col("user_age_days") === 7, 1).otherwise(0)).alias("retention_7"))

    val outputPath = dayAgoWriteDataOutPath(conf)

    bikeUserAggDf.coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }
}
