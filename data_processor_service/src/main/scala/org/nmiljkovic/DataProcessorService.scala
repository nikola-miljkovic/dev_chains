package org.nmiljkovic

import com.redis._

import scala.collection.immutable.HashMap

class DataProcessorService {
  private val CLEAN_REPO_SET = "clean:repo:set"
  private val CLEAN_REPO_PREFIX = "clean:repo"

  private val r = new RedisClient("localhost", 6379)
  private val topRepos = TakeTopRepos(40)
  private val cleanRepos = ClearRepoPaths()

  private val newCleanRepoSet = cleanRepos.keys.toSet
  private val oldCleanRepoSet = r.smembers(CLEAN_REPO_SET).orNull.collect { case Some(key) => key }

  println(s"Starting transaction for updating sets.")

  r.pipeline { c =>
    c.del(CLEAN_REPO_SET)
    cleanRepos.toList.sortBy(- _._2).foreach { pair =>
      c.sadd(CLEAN_REPO_SET, pair._1)
      c.set(s"$CLEAN_REPO_PREFIX:${pair._1}", pair._2)
    }
  }

  println(s"Transaction completed.")
  println(s"Total clean repositories: ${newCleanRepoSet.size}")
  println(s"Starting cleanup.")

  private val reposToDelete = oldCleanRepoSet.diff(newCleanRepoSet)

  reposToDelete.foreach { key =>
    r.del(key)
  }

  println(s"Cleanup completed. Total keys removed: ${reposToDelete.size}")

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

  private def removeLanguagesBelowThreshold(acc: HashMap[String, Int], pair: (String, Int)): HashMap[String, Int] = {
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

  private def ClearRepoPaths(): HashMap[String, Int] = {
    val KEY_PREFIX = "repo:path:"
    val KEY_PREFIX_SIZE = KEY_PREFIX.length

    CollectKeys(KEY_PREFIX)
      .map { key => (key.substring(KEY_PREFIX_SIZE), Integer.parseInt(r.get(key).get))}
      .foldLeft(HashMap[String, Int]())(removeLanguagesBelowThreshold)
  }
}
