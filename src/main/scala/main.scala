import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, FileUtil, Path}

import java.io.BufferedInputStream
import java.net.URI

object main extends App{
  val CreatedDir = "/ods"
  val ExistDir = "/stage"
  val conf = new Configuration()
  val fileSystem = FileSystem.get(new URI("hdfs://namenode:9000"), conf)

  def openFile(fileName: String): FSDataInputStream = {
    val path = new Path(fileName)
    fileSystem.open(path)
  }

  def MakeDir(folderPath: String) = {
    val dirName = new Path(folderPath)
    if (!fileSystem.exists(dirName)) {
      fileSystem.mkdirs(dirName)
    }
  }

  def MakeFile(filePath: String) = {
    val fileName = new Path(filePath)
    if (!fileSystem.exists(fileName)) {
      fileSystem.mkdirs(fileName)
    }
  }

  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = fileSystem.listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }
  def getFileNames(path: String): Array[String] = {
    getFilesAndDirs(path)
      .filter(fileSystem.getFileStatus(_).isFile())
      .map(_.getName)
  }
  def getDirNames(path: String): Array[String] = {
    getFilesAndDirs(path)
      .filter(fileSystem.getFileStatus(_).isDirectory)
      .map(_.getName)
  }

  def saveResult(files: Array[String], fromDirPath: String, toDirPath: String): Unit = {
    val fileName = files.head
    val newFilePath = s"$toDirPath/$fileName"

    MakeFile(newFilePath)

    val outFile = fileSystem.append(new Path(newFilePath))
    for (file <- files) {
      val currentFilePath = s"$fromDirPath/$file"
      val inFile = new BufferedInputStream(openFile(currentFilePath))
      val b = new Array[Byte](1024)
      var numBytes = inFile.read(b)
      while (numBytes > 0) {
        outFile.write(b, 0, numBytes)
        numBytes = inFile.read(b)
      }
      inFile.close()
    }
    outFile.close()
  }

  def main(CreatedDir: String, ExistDir: String): Unit = {

    MakeDir(CreatedDir)
    getDirNames(ExistDir).foreach { dir =>

      val currentDirPath = s"$ExistDir/$dir"
      val newDirPath = s"$CreatedDir/$dir"
      MakeDir(newDirPath)

      val files = getFileNames(currentDirPath)
        .filter(!_.contains(".inprogress"))

      if (files.length > 0) {
        saveResult(files, currentDirPath, newDirPath)
      }
    }
  }

  main(CreatedDir, ExistDir)
}

