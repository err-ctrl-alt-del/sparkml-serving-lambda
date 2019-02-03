package com.github.amazonaws.sparkml

import java.io.{ByteArrayOutputStream, File, FileInputStream}

import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class MleapLambdaRequestHandlerTest extends FunSuite with BeforeAndAfterAll {

  private val handlerInstance: RequestStreamHandler = new MleapLambdaRequestHandler()

  test("MleapLambdaRequestHandler.getBundle") {
    assert(MleapLambdaRequestHandler.getBundle(null).nonEmpty)
  }

  test("MleapLambdaRequestHandler.handleRequest") {
    val inputFile = new File(AwsResourceConfiguration.getAwsS3ModelResourceConfig._4 + "/my-model-data/frame.json")
      .getAbsolutePath
    val outputStream = new ByteArrayOutputStream()
    handlerInstance.handleRequest(new FileInputStream(inputFile), outputStream, null)
    assert(outputStream.size() > 0)
    println(new String(outputStream.toByteArray))
  }

}
