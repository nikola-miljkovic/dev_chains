package org.nmiljkovic

import com.redis._

import scala.collection.immutable.HashMap

class DataProcessorService {
  private val r = new RedisClient("localhost", 6379)
  private val topRepos = TakeTopRepos(40)
  ClearRepoPaths()
    .toList.sortBy(- _._2).foreach(k => println(s"${k._1} : ${k._2}"))

  println("-----------------")
  println("")
  topRepos.foreach(k => println(s"${k._1} : ${k._2}"))

  private def CollectKeys(keyPrefix: String) = r.keys(s"$keyPrefix*").orNull
      .collect { case Some(key) => key }

  private def TakeTopRepos(n: Int): Map[String, Int] = {
    val KEY_PREFIX = "repo:count:"
    val KEY_PREFIX_SIZE = KEY_PREFIX.length

    CollectKeys(KEY_PREFIX)
      .map { key => (key.substring(KEY_PREFIX_SIZE), Integer.parseInt(r.get(key).get)) }
      .sortBy(- _._2) // Reverse sort
      .take(n)
      .toMap
  }

  private def removeLanguagesBelowThreshold(acc: HashMap[String, Int], pair: (String, Int)) = {
    val (path, count) = pair
    val cleanPath = path.split(":")
      .foldLeft(List[String]())((acc, lang) => {
        if (topRepos.contains(lang)) lang :: acc else acc
      })
      .distinct
      .reverse // restore order
      .mkString(":")

    if (acc.contains(cleanPath)) {
      (acc - cleanPath) + (cleanPath -> (acc(cleanPath) + count))
    } else {
      acc + (cleanPath -> count)
    }
  }

  private def ClearRepoPaths() = {
    val KEY_PREFIX = "repo:path:"
    val KEY_PREFIX_SIZE = KEY_PREFIX.length

    CollectKeys(KEY_PREFIX)
      .map { key => (key.substring(KEY_PREFIX_SIZE), Integer.parseInt(r.get(key).get))}
      .foldLeft(HashMap[String, Int]())(removeLanguagesBelowThreshold)
  }
}
