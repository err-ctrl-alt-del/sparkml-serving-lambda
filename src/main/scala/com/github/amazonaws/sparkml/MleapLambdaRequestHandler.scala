package com.github.amazonaws.sparkml

import java.io._
import java.nio.charset.StandardCharsets

import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.GetObjectRequest
import com.github.amazonaws.sparkml.MleapLambdaRequestHandler.getBundle
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.runtime.serialization.FrameReader
import org.apache.commons.io.{FileUtils, IOUtils}
import resource.managed

import scala.util.{Failure, Success, Try}

class MleapLambdaRequestHandler extends RequestStreamHandler {

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    try {
      val inputAsString = IOUtils.toString(input, StandardCharsets.UTF_8)
      val inputFrame = FrameReader("ml.combust.mleap.json").fromBytes(inputAsString.getBytes())
      inputFrame match {
        case Success(i) =>
          val outputFrame = new MleapPipelineTransformer(getBundle(context)).transform(i)
          createOutputContent(output, context, outputFrame)
        case Failure(_) => context.getLogger.log("Failed to create frame from input payload")
      }
    } catch {
      case e : IOException =>
        context.getLogger.log("Failed to download bundle")
        throw e
    }
  }

  private def createOutputContent(output: OutputStream, context: Context, outputFrame: Try[DefaultLeapFrame]): Unit = {
    outputFrame match {
      case Success(o) =>
        for (bytes <- o.writer("ml.combust.mleap.json").toBytes();
             _ <- FrameReader("ml.combust.mleap.json").fromBytes(bytes)) {
          output.write(bytes)
      }
      case Failure(_) => context.getLogger.log("Failed to transform frame")
    }
  }

}

object MleapLambdaRequestHandler {

  private var bundle: Option[Bundle[Transformer]] = _

  @throws[IOException]
  def getBundle(context: Context): Option[Bundle[Transformer]] = {
    if (bundle == null)
      try {
        bundle = fetchDownloadedBundle
      } catch {
        case _: Exception => context.getLogger.log("Failed to fetch bundle from local directory")
      }
    if (bundle == null)
      try {
        bundle = downloadBundleFromS3
      } catch {
        case _: Exception => throw new IOException("Failed to download bundle from S3")
      }
    bundle
  }

  private def downloadBundleFromS3: Option[Bundle[Transformer]] = {
    val s3Client = new AmazonS3Client()
    val s3Object = s3Client.getObject(new GetObjectRequest(AwsResourceConfiguration.getAwsS3ModelResourceConfig._2,
      AwsResourceConfiguration.getAwsS3ModelResourceConfig._1))
    FileUtils.copyInputStreamToFile(new BufferedInputStream(s3Object.getObjectContent),
      new File(AwsResourceConfiguration.getAwsS3ModelResourceConfig._4 + "/" +
        AwsResourceConfiguration.getAwsS3ModelResourceConfig._1))
    fetchDownloadedBundle
  }

  private def fetchDownloadedBundle: Option[Bundle[Transformer]] = (for (
    bundleFile <- managed(BundleFile(new File(
      new File(AwsResourceConfiguration.getAwsS3ModelResourceConfig._4).getAbsolutePath + "/" +
      AwsResourceConfiguration.getAwsS3ModelResourceConfig._1)))) yield {
    bundleFile.loadMleapBundle().get
  }).opt

}