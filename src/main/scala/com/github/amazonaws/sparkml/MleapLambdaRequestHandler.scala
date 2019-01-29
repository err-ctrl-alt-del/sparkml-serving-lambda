package com.github.amazonaws.sparkml

import java.io.{BufferedInputStream, File, InputStream, OutputStream}
import java.nio.charset.StandardCharsets

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client}
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.runtime.serialization.FrameReader
import org.apache.commons.io.{FileUtils, IOUtils}
import resource.managed

import scala.util.{Failure, Success, Try}

class MleapLambdaRequestHandler extends RequestStreamHandler {

  {
    Try(downloadBundleFromS3) match {
      case Success(_) =>
      case Failure(_) => println("Failed to download bundle")
    }
  }

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    val inputAsString = IOUtils.toString(input, StandardCharsets.UTF_8)
    val inputFrame = FrameReader("ml.combust.mleap.json").fromBytes(inputAsString.getBytes())
    val mleapPipeline = new MleapPipelineTransformer(getBundle)
    inputFrame match {
      case Success(i) =>
        val outputFrame = mleapPipeline.transform(i)
        createOutputContent(output, context, outputFrame)
      case Failure(_) => context.getLogger.log("Failed to create frame from input payload")
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

  def downloadBundleFromS3(): Unit = {
    val s3Client: AmazonS3 = new AmazonS3Client()
    s3Client.setRegion(Region.getRegion(Regions.fromName(ResourceConfiguration.getAwsS3Config._3)))
    val s3Object = s3Client.getObject(new GetObjectRequest(ResourceConfiguration.getAwsS3Config._2,
      ResourceConfiguration.getAwsS3Config._1))
    FileUtils.copyInputStreamToFile(new BufferedInputStream(s3Object.getObjectContent),
      new File(ResourceConfiguration.getAwsS3Config._4 + "/" + ResourceConfiguration.getAwsS3Config._1))
  }

  private def getBundle: Option[Bundle[Transformer]] = (for (
    bundleFile <- managed(BundleFile("jar:file:" +
      ResourceConfiguration.getAwsS3Config._4 + "/" + ResourceConfiguration.getAwsS3Config._1))) yield {
    bundleFile.loadMleapBundle().get
  }).opt

}
