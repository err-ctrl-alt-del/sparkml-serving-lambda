package com.github.amazonaws.sparkml

import java.io.File
import java.nio.charset.StandardCharsets

import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.serialization.{FrameReader, FrameWriter}
import resource.managed
import org.scalatest.FunSuite

class MleapLambdaRequestHandlerTest extends FunSuite {

  def getBundle: Option[Bundle[Transformer]] = (for (
    bundleFile <- managed(BundleFile(new File(
      "src/test/resources/" + AwsResourceConfiguration.getAwsS3Config._1)))) yield {
    bundleFile.loadMleapBundle().get
  }).opt

  test("MleapLambdaRequestHandlerTest.schema") {
    assert(new MleapPipelineTransformer(getBundle).schema.get.fields.nonEmpty)
  }

  test("MleapLambdaRequestHandlerTest.transform") {
    val s = scala.io.Source.fromFile(new File(
      "src/test/resources/my-model-data/frame.json").getAbsolutePath).mkString
    val inputFrame = FrameReader("ml.combust.mleap.json").fromBytes(s.getBytes(StandardCharsets.UTF_8))
    val mleapPipeline = new MleapPipelineTransformer(getBundle)
    val outputFrame = mleapPipeline.transform(inputFrame.get).get
    val data = outputFrame.dataset
    for(bytes <- outputFrame.writer("ml.combust.mleap.json").toBytes();
        _ <- FrameReader("ml.combust.mleap.json").fromBytes(bytes)) {
      println(new String(bytes))
    }
    assert(data.nonEmpty)
  }

}
