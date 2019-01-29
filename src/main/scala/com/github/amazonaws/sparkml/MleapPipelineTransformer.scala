package com.github.amazonaws.sparkml

import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}

import scala.util.{Failure, Success, Try}

class MleapPipelineTransformer(var bundle: Option[Bundle[Transformer]]) {

  def transform(frame: DefaultLeapFrame): Try[DefaultLeapFrame] = synchronized {
    bundle.map {
      _.root.transform(frame)
    }.getOrElse(Failure(new IllegalStateException("no transformer loaded")))
  }

  def schema: Try[StructType] = synchronized {
    bundle.map {
      b => Success(b.root.schema)
    }.getOrElse(Failure(new IllegalStateException("no transformer loaded")))
  }

}