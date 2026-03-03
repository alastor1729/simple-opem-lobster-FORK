package com.bhp.dp.utils

import scala.annotation.tailrec
import scala.collection.mutable

class PathTree[T] {
  private final val paths = mutable.Map.empty[String, PathNode]
  private final val items = mutable.ListBuffer.empty[T]

  def addConfig(path: Seq[String], fileType: String, data: T): Unit = {
    addDirectNodeToPaths(paths, pathSplitToNode(path, fileType, Some(data)))
    items += data
  }

  def locateDataForPath(path: Seq[String], fileType: String): Option[T] = {
    if (path.isEmpty) None else locateDataInPaths(paths, pathSplitToNode(path, fileType, None))
  }

  def getItems: Seq[T] = {
    items
  }

  @tailrec
  private def addDirectNodeToPaths(p: mutable.Map[String, PathNode], toAdd: DirectNode): Unit = {
    if (!p.contains(toAdd.nodeName)) {
      // no matching path, add
      p += (toAdd.nodeName -> toAdd.toPathNode)
    } else if (toAdd.childNode.isEmpty) {
      // matching path, but no ChildNode to add, set new filetype and data
      val fileTypeToData = p(toAdd.nodeName).fileTypeToData

      if (fileTypeToData.contains(toAdd.fileType) && fileTypeToData(toAdd.fileType).isDefined) {
        // node already contains data for that filetype. throw exception.
        throw new Exception(
          s"Tried to add new data for file type ${toAdd.fileType} in path ${toAdd.fullPath} where one already existed."
        )
      }

      fileTypeToData += (toAdd.fileType -> toAdd.data)
    } else {
      // matching path, more children to add, continue
      addDirectNodeToPaths(p(toAdd.nodeName).children, toAdd.childNode.get);
    }
  }

  private def locateDataInPaths(p: mutable.Map[String, PathNode], toFind: DirectNode): Option[T] = {
    p.get(toFind.nodeName)
      .flatMap(matchingPath => {
        lazy val currentType = matchingPath.fileTypeToData.get(toFind.fileType).flatten
        toFind.childNode match {
          case Some(child) => locateDataInPaths(matchingPath.children, child).orElse(currentType)
          case None        => currentType
        }
      })
  }

  private def pathSplitToNode(
      pathItems: Seq[String],
      fileType: String,
      data: Option[T]
  ): DirectNode = {
    pathItems
      .foldRight(Option.empty[DirectNode]) { case (pathElement, childNode) =>
        Some(DirectNode(pathElement, fileType, data, pathItems, childNode))
      }
      .getOrElse(throw new Exception(s"Tried to create node with empty path. $pathItems"))
  }

  case class PathNode(
      nodeName: String,
      fileTypeToData: mutable.Map[String, Option[T]],
      children: mutable.Map[String, PathNode]
  )
  case class DirectNode(
      nodeName: String,
      fileType: String,
      data: Option[T],
      fullPath: Seq[String],
      childNode: Option[DirectNode] = None
  ) {
    def toPathNode: PathNode = {
      PathNode(
        nodeName = nodeName,
        fileTypeToData = childNode match {
          case Some(_) => mutable.Map.empty
          case None    => mutable.Map(fileType -> data)
        },
        children =
          childNode.map(c => mutable.Map(c.nodeName -> c.toPathNode)).getOrElse(mutable.Map.empty)
      )
    }
  }
}
