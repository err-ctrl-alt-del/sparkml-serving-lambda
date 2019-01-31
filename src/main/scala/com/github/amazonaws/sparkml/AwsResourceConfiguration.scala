package com.github.amazonaws.sparkml

import com.typesafe.config.{Config, ConfigFactory}

object AwsResourceConfiguration {

  private var awsConfig: Config = _

  def loadAwsConfig: Config = {
    if (awsConfig == null)
      awsConfig = ConfigFactory.parseResources("aws.conf").resolve()
    awsConfig
  }

  def getAwsS3Config: (String, String, String, String) = {
    val key = loadAwsConfig.getString("s3.key")
    val bucketName = loadAwsConfig.getString("s3.bucket-name")
    val regionName = loadAwsConfig.getString("s3.region-name")
    val localParentDir = loadAwsConfig.getString("s3.local-parent-dir")
    (key, bucketName, regionName, localParentDir)
  }

}
