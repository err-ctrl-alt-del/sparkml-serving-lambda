
name := "sparkml-serving-lambda"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-lambda-java-core" % "1.2.0",
  "com.amazonaws" % "aws-lambda-java-events" % "1.2.0",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.294",
  "ml.combust.mleap" %% "mleap-runtime" % "0.12.0",
  "commons-io" % "commons-io" % "2.6",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

assemblyMergeStrategy in assembly := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
