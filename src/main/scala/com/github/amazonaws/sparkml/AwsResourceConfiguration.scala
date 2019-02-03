package com.github.amazonaws.sparkml

import com.typesafe.config.{Config, ConfigFactory}

object AwsResourceConfiguration {

  private lazy val awsConfig: Config = ConfigFactory.parseResources("aws.conf").resolve()

  def getAwsS3ModelResourceConfig: (String, String, String, String) = {
    (awsConfig.getString("s3.key"), awsConfig.getString("s3.bucket-name"),
      awsConfig.getString("s3.region-name"), awsConfig.getString("s3.local-parent-dir"))
  }

}
