package com.github.amazonaws.sparkml

import java.io._
import java.nio.charset.StandardCharsets

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
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

  var bundle: Option[Bundle[Transformer]] = _

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    try {
      val inputAsString = IOUtils.toString(input, StandardCharsets.UTF_8)
      val inputFrame = FrameReader("ml.combust.mleap.json").fromBytes(inputAsString.getBytes())
      val mleapPipeline = new MleapPipelineTransformer(getBundle)
      inputFrame match {
        case Success(i) =>
          val outputFrame = mleapPipeline.transform(i)
          createOutputContent(output, context, outputFrame)
        case Failure(_) => context.getLogger.log("Failed to create frame from input payload")
      }
    } catch {
      case _: IOException =>
        context.getLogger.log("Failed to download bundle")
        throw new IOException()
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
  def getBundle: Option[Bundle[Transformer]] = {
    if (bundle.isEmpty)
      Try(bundle = downloadBundleFromS3) match {
        case Failure(_) => throw new IOException("Failed to download bundle")
      }
    bundle
  }

  private def downloadBundleFromS3: Option[Bundle[Transformer]] = {
    val s3Client: AmazonS3 = new AmazonS3Client()
    s3Client.setRegion(Region.getRegion(Regions.fromName(AwsResourceConfiguration.getAwsS3Config._3)))
    val s3Object = s3Client.getObject(new GetObjectRequest(AwsResourceConfiguration.getAwsS3Config._2,
      AwsResourceConfiguration.getAwsS3Config._1))
    FileUtils.copyInputStreamToFile(new BufferedInputStream(s3Object.getObjectContent),
      new File(AwsResourceConfiguration.getAwsS3Config._4 + "/" + AwsResourceConfiguration.getAwsS3Config._1))
    fetchDownloadedBundle
  }

  private def fetchDownloadedBundle: Option[Bundle[Transformer]] = (for (
    bundleFile <- managed(BundleFile("jar:file:" +
      AwsResourceConfiguration.getAwsS3Config._4 + "/" + AwsResourceConfiguration.getAwsS3Config._1))) yield {
    bundleFile.loadMleapBundle().get
  }).opt

}