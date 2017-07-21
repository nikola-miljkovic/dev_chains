package org.nmiljkovic

import com.lambdaworks.redis.api.async.RedisAsyncCommands
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import khttp.get
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.util.*

class UserConsumer(channel: Channel, val redisAsync: RedisAsyncCommands<String, String>) : DefaultConsumer(channel) {

    companion object {
        val logger = LoggerFactory.getLogger(UserConsumer::class.java)!!
    }

    private fun visitUser(url: String) {
        val req = get(url)

        if (req.statusCode == 403) {
            val limitReset = req.headers["X-RateLimit-Reset"]!!.toLong()
            logger.info("GitHub API limit reached, pausing until ${Date(limitReset * 1000)}.")

            val currentTime = Date().time
            val timeUntilReset = (limitReset + 1) * 1000 - currentTime
            Thread.sleep(timeUntilReset)

            logger.info("GitHub API limit has been reset, continuing.")
            return visitUser(url)
        }

        val repos = req.jsonArray
        if (repos.length() <= 2) {
            return
        }

        val languagesClean = repos.filter {
            // filters nulls
            it as JSONObject
            it["language"] is String
        }.map {
            it as JSONObject
            it["language"] as String
        }

        val validReposCount = languagesClean.size
        if (validReposCount <= 2) {
            return
        } else if (validReposCount > 100) {
            logger.info("Insane number of repos -> $url")
        }

        val startingLanguage = languagesClean[0]
        val languages = languagesClean.drop(1)
                .fold(MutableList(1, { startingLanguage })) { list, el ->
                    // don't add sequential duplicates
                    val tailEl = list.last()
                    if (tailEl == el) {
                        redisAsync.incr("repo:strong:count:$el")
                    } else {
                        list.add(el)
                        redisAsync.incr("repo:count:$el")
                        redisAsync.incr("repo:edge:$tailEl:$el")
                    }
                    list
                }

        redisAsync.incrby("repo:total_count", languages.size.toLong())
        redisAsync.incr("repo:path:${languages.joinToString(":")}")

        redisAsync.incr("repo:first:$startingLanguage")
        redisAsync.incr("repo:last:${languages.last()}")
    }

    override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
        if (body != null) {
            val userName = body.toString(Charset.defaultCharset())
            val apiQuery = "type=all&sort=created&direction=asc&access_token=ec7f73570229792d7a428092bf84afa0d187d9bd"
            val apiRequest = "https://api.github.com/users/$userName/repos?$apiQuery"

            logger.info("Constructing path for User: $userName")

            visitUser(apiRequest)

            channel.basicAck(envelope!!.deliveryTag, false)
        }
    }
}