import AssemblyKeys._

Statika.distributionProject

name := "bayenv"

description := ""

organization := "ohnosequences"

libraryDependencies ++= Seq(
  "ohnosequences" % "nispero-abstract_2.10" % "1.0.2"
)

metadataObject := name.value

dependencyOverrides += "ohnosequences" % "aws-statika_2.10" % "1.0.1"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case "about.html" => MergeStrategy.first
    case x => old(x)
  }
}
