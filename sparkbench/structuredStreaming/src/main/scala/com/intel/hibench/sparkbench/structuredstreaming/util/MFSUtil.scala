package com.intel.hibench.sparkbench.structuredstreaming.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

object MFSUtil {
  val HadoopConfDir = "/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/"
  val fs: FileSystem = FileSystem.get(createHadoopConfiguration())


  def createHadoopConfiguration(): Configuration = {
    val conf = new Configuration
    val uri = "maprfs:///"
     // Setting HDFS2 properties:
      try { // Load mapred-site and yarn-site from mapr directory
        val mapredSite = HadoopConfDir + "mapred-site.xml"
        conf.addResource(new Path(mapredSite))
        val yarnSite = HadoopConfDir + "yarn-site.xml"
        conf.addResource(new Path(yarnSite))
      } catch {
        case e: Exception =>
          // We dont want to fail this
          e.printStackTrace()
      }

    conf
  }

  def write(text: String, path: Path): Unit = {
    if (!fs.exists(path)) {
      write(fs.create(path, true))
    } else {
      write(fs.append(path))
    }


    def write(outputStream: FSDataOutputStream): Unit = {
      outputStream.writeBytes(s"$text")
      outputStream.close()
    }
  }
  def exists(path: Path) = fs.exists(path)

  def deleteIfExists(path: Path) = {
    if (exists(path)) {
      fs.delete(path, true)
    }
  }

  def initFolder(path: Path) = {
    deleteIfExists(path)
    fs.mkdirs(path)
  }

}
