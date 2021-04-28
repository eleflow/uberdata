/*
 * Copyright 2015 eleflow.com.br.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eleflow.uberdata.notebook

import java.io.{IOException, InputStream, OutputStream}
import java.net.URI

import com.google.gson.{Gson, GsonBuilder}
import org.apache.commons.io.IOUtils
import org.apache.commons.vfs2._
import org.apache.zeppelin.conf.ZeppelinConfiguration
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars
import org.apache.zeppelin.notebook.{Note, NoteInfo}
import org.apache.zeppelin.notebook.repo.{NotebookRepo, VFSNotebookRepo}
import org.apache.zeppelin.scheduler.Job.Status
import org.apache.zeppelin.user.AuthenticationInfo
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.io.File

/**
  * Created by dirceu on 18/09/15.
  */
class UberdataNotebookRepo(conf: ZeppelinConfiguration) extends VFSNotebookRepo(conf) {
  private val log: Logger =
    LoggerFactory.getLogger(classOf[UberdataNotebookRepo])
  private lazy val fsManager: FileSystemManager = VFS.getManager

  private val renameOldRepos = {
    println("renameOldRepos")
    import scala.collection.JavaConversions._
    val allNotes = list()
    allNotes.filter { notebook =>
      notebook.getName != notebook.getId && !resolveFile(notebook.getName).exists()
    }.foreach { notebook =>
      val oldRepo = resolveFile(notebook.getId)
      val localNotes = notebooks
      val folders = notebook.getName
      val noteFolder = createFolders(folders)
      val noteFile: FileObject =
        noteFolder.resolveFile("note.json", NameScope.CHILD)
      val json: String = gson.toJson(getNote(oldRepo))
      val out: OutputStream = noteFile.getContent.getOutputStream(false)
      out.write(json.getBytes(conf.getString(ConfVars.ZEPPELIN_ENCODING)))
      out.close
      localNotes.filter(f => f._2 != notebook.getName && f._1 == notebook.getId()).map { f =>
        createFolders(f._2).delete(allFileSelector)
        cleanParentFolders(f._2)
      }

      remove(notebook.getId)
      oldRepo.delete(new AllFileSelector)
    }
    true
  }

  lazy val filesystemRoot = new URI(conf.getNotebookDir() match {
    case "notebook" => s"${new java.io.File(".").getAbsolutePath}/notebook"
    case s: String => {
      log.warn(s)
      s
    }
  })
  lazy val rootDir = getRootDir

  private def resolveFile(noteId: String) = {
    val rootDir: FileObject = fsManager.resolveFile(getPath("/"))
    rootDir.resolveFile(noteId, NameScope.CHILD)
  }

  @throws(classOf[IOException])
  override def remove(noteId: String, subject: AuthenticationInfo = ???): Unit = {
    val noteDir = resolveFile(noteId)
    if (!noteDir.exists) {
      return
    }
    if (!isDirectory(noteDir)) {
      throw new IOException("Can not remove " + noteDir.getName.toString)
    }
    noteDir.delete(Selectors.SELECT_SELF_AND_CHILDREN)
  }

  @throws(classOf[IOException])
  override def list(
    subject: AuthenticationInfo = ???
  ): java.util.List[NoteInfo] = {
    import scala.collection.JavaConversions._

    children.filter { f =>
      val fileName: String = f.getName.getBaseName
      !(f.isHidden || fileName.startsWith(".") || fileName.startsWith("#") || fileName.startsWith(
        "~") || !isDirectory(f))
    }.map {
      case f =>
        try {
          getNoteInfo(f)
        } catch {
          case e: IOException => {
            log.error("Can't read note " + f.getName.toString, e)
            null
          }
          case e: Exception => {
            log.error("Can't read note " + f.getName.toString, e)
            null
          }
        }
    }.filter(_ != null).toList

  }

  def children = getChildren(rootDir)

  import scala.collection.JavaConversions._

  def notebooks =
    list().map { f =>
      (f.getId -> f.getName)
    }.toMap

  private def getChildren(rootDir: FileObject): Array[FileObject] = {
    if (isDirectory(rootDir)) {
      val children: Array[FileObject] = rootDir.getChildren
      val (folders, _) = children.partition { f =>
        isDirectory(f)
      }
      val x =
        folders.headOption.map(_ => folders flatMap (getChildren(_))).getOrElse(Array(rootDir))
      x
    } else Array(rootDir.getParent)
  }

  @throws(classOf[IOException])
  private def getNoteInfo(noteDir: FileObject): NoteInfo = {
    val note: Note = getNote(noteDir)
    return new NoteInfo(note)
  }

  override def get(noteId: String, subject: AuthenticationInfo): Note =
    getNote(noteId).map(getNote(_)).headOption.getOrElse(throw new Exception("Notebook not found"))

  private def getNote(noteId: String) = {
    import scala.collection.JavaConversions._
    list().filter(f => f.getId == noteId).map { f =>
      resolveFile(f.getName)
    }
  }

  @throws(classOf[IOException])
  private def getNote(noteDir: FileObject): Note = {
    if (!isDirectory(noteDir)) {
      throw new IOException(noteDir.getName.toString + " is not a directory")
    }
    val noteJson: FileObject =
      noteDir.resolveFile("note.json", NameScope.CHILD)
    if (!noteJson.exists) {
      throw new IOException(noteJson.getName.toString + " not found")
    }
    val gsonBuilder: GsonBuilder = new GsonBuilder
    gsonBuilder.setPrettyPrinting
    val gson: Gson = gsonBuilder.create
    val content: FileContent = noteJson.getContent
    val ins: InputStream = content.getInputStream
    val json: String =
      IOUtils.toString(ins, conf.getString(ConfVars.ZEPPELIN_ENCODING))
    ins.close
    val note: Note = gson.fromJson(json, classOf[Note])
    import scala.collection.JavaConversions._
    for (p <- note.getParagraphs) {
      if (p.getStatus == Status.PENDING || p.getStatus == Status.RUNNING) {
        p.setStatus(Status.ABORT)
      }
    }
    return note
  }

  private lazy val gsonBuilder = new GsonBuilder
  private lazy val gson = gsonBuilder.create()

  override def save(note: Note, subject: AuthenticationInfo): Unit =
    if (note.getName != null) {
      val localNotes = notebooks
      val pattern = s"""${note.getName}\\-V(\\d{1,10})""".r
      val sameNameNotes = localNotes.filter { f =>
        (f._2 == note.getName || f._2.startsWith(note.getName) && pattern
          .findFirstIn(f._2)
          .isDefined) && f._1 != note.getId
      }
      sameNameNotes.headOption.map { _ =>
        note.setName(s"${note.getName}-V${sameNameNotes.size + 1}")
      }
      val folders = note.getName
      val noteFolder = createFolders(folders)
      val noteFile: FileObject =
        noteFolder.resolveFile("note.json", NameScope.CHILD)
      val json: String = gson.toJson(note)
      val out: OutputStream = noteFile.getContent.getOutputStream(false)
      out.write(json.getBytes(conf.getString(ConfVars.ZEPPELIN_ENCODING)))
      out.close
      localNotes.filter(f => f._2 != note.getName && f._1 == note.getId).map { f =>
        createFolders(f._2).delete(allFileSelector)
        cleanParentFolders(f._2)

      }
    }

  lazy val allFileSelector = new AllFileSelector

  private def cleanParentFolders(folder: String): Unit = {
    if (folder.indexOf("/") > 0) {
      val superFolderName = folder.substring(0, folder.lastIndexOf("/"))
      val superFolder = createFolders(superFolderName)

      if (superFolder.findFiles(allFileSelector).size == 1) {
        superFolder.delete(allFileSelector)
        cleanParentFolders(superFolderName)
      }
    }

  }

  //
  //  def getFolders(name: String) = {
  //    name.split(File.pathSeparator) //:+ note.id()
  //  }

  def createFolders(folders: String) = {
    Seq(folders).foldLeft(rootDir) {
      case (last, current) =>
        val currentFile = last.resolveFile(current)
        if (!currentFile.exists()) currentFile.createFolder()
        currentFile
    }
  }

  @throws(classOf[IOException])
  override def getRootDir: FileObject = {
    val rootDir: FileObject = fsManager.resolveFile(getPath("/"))
    if (!rootDir.exists) {
      throw new IOException("Root path does not exists")
    }
    if (!isDirectory(rootDir)) {
      throw new IOException("Root path is not a directory")
    }
    return rootDir
  }

  private def getPath(path: String) =
    if (path == null || path.trim.length == 0) {
      filesystemRoot.toString
    } else if (path.startsWith("/")) {
      filesystemRoot.toString + path
    } else {
      filesystemRoot.toString + "/" + path
    }

  private def isDirectory(fo: FileObject) =
    fo != null && fo.getType() == FileType.FOLDER
}
