package org.nmiljkovic

import com.jdiazcano.cfg4k.loaders.PropertyConfigLoader
import com.jdiazcano.cfg4k.providers.ProxyConfigProvider
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.async.RedisAsyncCommands
import com.rabbitmq.client.*
import khttp.get
import org.json.JSONObject
import org.slf4j.LoggerFactory
import spark.kotlin.Http
import spark.kotlin.ignite
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.Executors


data class ServiceConfig(val serviceName: String, val serviceUrl: String, val global: Map<String, String>)

fun getConfiguration(): ServiceConfig {
    val serviceConfigLoader = PropertyConfigLoader(System::class.java.getResource("/service.properties"))
    val serviceConfigProvider = ProxyConfigProvider(serviceConfigLoader)

    val serviceName = serviceConfigProvider.get("service.name", String::class.java)
    val serviceUrl = serviceConfigProvider.get("service.url", String::class.java)

    val globalConfigLoader = PropertyConfigLoader(System::class.java.getResource("/global.properties"))
    val globalConfigProvider = ProxyConfigProvider(globalConfigLoader)

    val global = HashMap<String, String>()
    global["redis.url"] = globalConfigProvider.get("redis.url", String::class.java)
    global["rabbitmq.url"] = globalConfigProvider.get("rabbitmq.url", String::class.java)
    global["rabbitmq.user"] = globalConfigProvider.get("rabbitmq.user", String::class.java)
    global["rabbitmq.pass"] = globalConfigProvider.get("rabbitmq.pass", String::class.java)

    return ServiceConfig(serviceName, serviceUrl, global)
}

fun getRabbitMqConnection(url: String, user: String, pass: String, vhost: String = "", numOfThreads: Int = 1): Connection {
    val factory = ConnectionFactory()
    val executor = Executors.newFixedThreadPool(numOfThreads)
    factory.setUri("amqp://$user:$pass@$url")
    return factory.newConnection(executor)
}

fun getHttp(url: String): Http {
    val endpointValues = url.split(':')
    return ignite()
            .ipAddress(endpointValues[0])
            .port(endpointValues[1].toInt())
}

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(::main.javaClass)!!
    val (serviceName, serviceUrl, globals) = getConfiguration()

    val http = getHttp(serviceUrl)
    val rabbitConn = getRabbitMqConnection(globals["rabbitmq.url"]!!, globals["rabbitmq.user"]!!,
            globals["rabbitmq.user"]!!, numOfThreads = 5)

    val redisClient = RedisClient.create("redis://${globals["redis.url"]}/0")
    val redisConn = redisClient.connect()

    val service = RepositoryAnalysisService(redisConn, rabbitConn.createChannel())
    logger.info("Starting service $serviceName")
    service.start()

    http.get("/status") {
        service.status.toString()
    }
}

class RepositoryAnalysisService(redisConn: StatefulRedisConnection<String, String>, val rabbitChan: Channel) {
    val redisAsync = redisConn.async()!!
    var status = ServiceStatus.Inactive

    companion object {
        val logger = LoggerFactory.getLogger(RepositoryAnalysisService::class.java)!!
    }

    private fun startService() {
        rabbitChan.exchangeDeclare("default", "direct", true)
        rabbitChan.queueDeclare("default", true, false, false, null)
        rabbitChan.queueBind("default", "default", "black")
        rabbitChan.basicConsume("default", true, UserConsumer(rabbitChan, redisAsync))
        rabbitChan.basicQos(10)

        status = ServiceStatus.Running

        logger.info("Service started.")
    }

    fun start() {
        if (status == ServiceStatus.Inactive) {
            startService()
        }
    }
}

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

enum class ServiceStatus {
    Inactive,

    Running,
}
