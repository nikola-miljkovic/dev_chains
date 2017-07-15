package org.nmiljkovic

import com.lambdaworks.redis.api.StatefulRedisConnection
import khttp.get
import org.json.JSONObject
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

private fun deserializeLinks(linkStr: String): Map<String?, String?>? {
    val regex = """<(.*)>;\s*rel="(\w+)"""".toRegex()
    val linesToMatch = linkStr.split(',')

    return linesToMatch.map {
        val groups = regex.matchEntire(it.trim())?.groups
        groups?.get(2)?.value to groups?.get(1)?.value
    }.toMap()
}

class UserTraversalService(val redisConn: StatefulRedisConnection<String, String>) {
    private var done = AtomicInteger()

    private data class SearchResponse(val nextLink: String?, val usersList: Map<String, String>, val count: Int,
                                      val limitRemaining: Int, val limitResetTime: Long)

    companion object {
        private const val API_ENDPOINT = "https://api.github.com/users?per_page=100"
    }

    private val traverseUsers = fun (): Unit {
        fun visitPage(url: String): SearchResponse {
            val req = get(url + "&access_token=7b53804a9d1362b3313c8f5198cc884966f0817d")

            val limitRemaining = req.headers["X-RateLimit-Remaining"]!!.toInt()
            val limitReset = req.headers["X-RateLimit-Reset"]!!.toLong()

            val links = req.headers["Link"]
            val linkList = if (links != null) deserializeLinks(links) else null
            val nextLink = linkList?.get("next")
            val data = req.jsonArray.map {
                it as JSONObject
                val key = "usr:${it["id"]}"
                val login = it["login"] as String
                key to login
            }.toMap()

            return SearchResponse(nextLink, data, data.size, limitRemaining, limitReset)
        }
        var nextEndpoint = API_ENDPOINT
        val asyncCommand = redisConn.async()

        while (true) {
            val (nextLink, usersList, count, limitRemaining, limitReset) = visitPage(nextEndpoint)
            done.addAndGet(count)

            usersList.forEach {
                asyncCommand.set(it.key, it.value)
            }

            if (nextLink != null)
                nextEndpoint = nextLink
            else
                break

            if (limitRemaining == 0) {
                val currentTime = Date().time
                val timeUntilReset = limitReset * 1000 - currentTime
                Thread.sleep(timeUntilReset)
            }
        }
    }

    private val traversalTask = Thread(traverseUsers)

    val progress: Int
        get() = done.get()

    fun Start(): ServiceStartResult {
        when (traversalTask.state) {
            Thread.State.NEW -> {
                traversalTask.start()
                return ServiceStartResult.Started
            }
            Thread.State.TERMINATED -> return ServiceStartResult.Finished
            Thread.State.TIMED_WAITING, Thread.State.WAITING -> return ServiceStartResult.Waiting
            else -> return ServiceStartResult.InProgress
        }
    }
}

enum class ServiceStartResult {
    // Service has just started execution
    Started,

    // Service has finished reading data
    Finished,

    // Service job is in progress
    InProgress,

    // Service job is paused because of rate limit
    Waiting
}