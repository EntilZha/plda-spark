import AssemblyKeys._

assemblySettings

name := "org/lda"

version := "1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1"

libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.7"

libraryDependencies += "org.scalanlp" % "breeze-natives_2.10" % "0.7"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case _ => MergeStrategy.first
}
}
