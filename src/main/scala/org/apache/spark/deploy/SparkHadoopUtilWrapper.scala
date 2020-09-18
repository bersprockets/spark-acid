package org.apache.spark.deploy

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.hadoop.mapred.JobConf

object SparkHadoopUtilWrapper {
  def appendS3AndSparkHadoopConfigurations(conf: SparkConf,
                                           hadoopConf: Configuration): Unit = {
    SparkHadoopUtil.get.appendS3AndSparkHadoopHiveConfigurations(conf, hadoopConf)
  }

  def addCredentials(conf: JobConf): Unit = {
    SparkHadoopUtil.get.addCredentials(conf)
  }
}