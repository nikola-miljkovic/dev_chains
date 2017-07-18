package org.nmiljkovic

import com.lambdaworks.redis.api.StatefulRedisConnection
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import khttp.get
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.net.SocketException
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class UserTraversalService(redisConn: StatefulRedisConnection<String, String>, val rabbitConn: Connection) {
    private var done = AtomicInteger()
    private val asyncCommand = redisConn.async()
    private val traversalTask = Thread(this::traverseUsers)

    val state: Thread.State
        get() = traversalTask.state

    var lastId: Int
        get() = asyncCommand.get("user:last_id").get()?.toInt() ?: 0
        set(value) {
            asyncCommand.set("user:last_id", value.toString())
        }

    private fun traverseUsers() {
        logger.info("Loading service connections.")
        val rabbitChan = rabbitConn.createChannel()
        var nextEndpoint = "$API_ENDPOINT&since=$lastId"

        val opts = HashMap<String, Any>()
        opts.put("x-max-length", 500)
        rabbitChan.exchangeDeclare("default", "direct", true)
        rabbitChan.queueDeclare("default", true, false, false, opts)
        rabbitChan.queueBind("default", "default", "black")

        logger.info("Service started.")
        while (true) {
            val (nextLink, usersList, count) = visitPage(nextEndpoint)
            done.addAndGet(count)

            usersList.forEach {
                try {
                    rabbitChan.basicPublish("default", "black", null, it.value.toByteArray())
                } catch (exc: SocketException) {
                    logger.error("RabbitMQ Connection failed!")
                }
            }
            lastId = usersList.minBy { it.key }!!.key

            if (nextLink != null)
                nextEndpoint = nextLink
            else
                break
        }
    }

    private data class SearchResponse(val nextLink: String?, val usersList: Map<Int, String>, val count: Int)

    companion object {
        val logger = LoggerFactory.getLogger(UserTraversalService::class.java)!!
        private const val API_ENDPOINT = "https://api.github.com/users?per_page=100"

        private fun deserializeLinks(linkStr: String): Map<String?, String?>? {
            val regex = """<(.*)>;\s*rel="(\w+)"""".toRegex()
            val linesToMatch = linkStr.split(',')

            return linesToMatch.map {
                val groups = regex.matchEntire(it.trim())!!.groups
                groups[2]!!.value to groups[1]!!.value
            }.toMap()
        }

        private fun visitPage(url: String): SearchResponse {
            val req = get(url + "&access_token=ec7f73570229792d7a428092bf84afa0d187d9bd")

            if (req.statusCode == 403) {
                val limitReset = req.headers["X-RateLimit-Reset"]!!.toLong()
                logger.info("GitHub API limit reached, pausing until ${Date(limitReset * 1000)}.")

                val currentTime = Date().time
                val timeUntilReset = (limitReset + 1) * 1000 - currentTime
                Thread.sleep(timeUntilReset)

                logger.info("GitHub API limit has been reset, continuing.")
                return visitPage(url)
            }

            val links = req.headers["Link"]
            val linkList = if (links != null) deserializeLinks(links) else null
            val nextLink = linkList?.get("next")
            val data = req.jsonArray.map {
                it as JSONObject
                val key = it["id"] as Int
                val login = it["login"] as String
                key to login
            }.toMap()

            return SearchResponse(nextLink, data, data.size)
        }
    }

    fun start(wipe: Boolean = false) {
        if (traversalTask.state == Thread.State.NEW) {
            if (wipe)
                lastId = 0

            traversalTask.start()
        }
    }
}