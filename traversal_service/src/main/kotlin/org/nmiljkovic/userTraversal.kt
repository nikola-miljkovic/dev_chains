package org.nmiljkovic

import com.jdiazcano.cfg4k.loaders.PropertyConfigLoader
import com.jdiazcano.cfg4k.providers.ProxyConfigProvider
import spark.kotlin.*
import com.lambdaworks.redis.RedisClient
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.json.JSONObject
import org.slf4j.LoggerFactory
import java.util.*

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

fun getRabbitMqConnection(url: String, user: String, pass: String): Connection {
    val factory = ConnectionFactory()
    factory.setUri("amqp://$user:$pass@$url")
    return factory.newConnection()
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
    val rabbitConn = getRabbitMqConnection(globals["rabbitmq.url"]!!, globals["rabbitmq.user"]!!, globals["rabbitmq.user"]!!)

    val redisClient = RedisClient.create("redis://${globals["redis.url"]}/0")
    val redisConn = redisClient.connect()

    val service = UserTraversalService(redisConn, rabbitConn)

    logger.info("Starting service.")
    service.start()

    http.get("/progress") {
        val json = JSONObject()
        json.put("service", serviceName)
        json.put("state", service.state)
        json.put("total_users", service.totalUsers)
        json.put("last_id", service.lastId)
        json
    }
}