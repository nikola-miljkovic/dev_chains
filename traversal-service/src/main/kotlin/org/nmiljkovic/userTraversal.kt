package org.nmiljkovic

import com.jdiazcano.cfg4k.loaders.PropertyConfigLoader
import com.jdiazcano.cfg4k.providers.ProxyConfigProvider
import spark.kotlin.*
import com.lambdaworks.redis.RedisClient

data class ServiceConfig(val serviceName: String, val serviceUrl: String, val redisUrl: String)

fun getConfiguration(): ServiceConfig {
    val serviceConfigLoader = PropertyConfigLoader(System::class.java.getResource("/service.properties"))
    val serviceConfigProvider = ProxyConfigProvider(serviceConfigLoader)

    val serviceName = serviceConfigProvider.get("service.name", String::class.java)
    val serviceUrl = serviceConfigProvider.get("service.url", String::class.java)

    val globalConfigLoader = PropertyConfigLoader(System::class.java.getResource("/global.properties"))
    val globalConfigProvider = ProxyConfigProvider(globalConfigLoader)

    val redisUrl = globalConfigProvider.get("redis.url", String::class.java)

    return ServiceConfig(serviceName, serviceUrl, redisUrl)
}

fun main(args: Array<String>) {
    val (serviceName, serviceUrl, redisUrl) = getConfiguration()

    val redisClient = RedisClient.create("redis://$redisUrl/0")
    val redisConn = redisClient.connect()
    val syncCommands = redisConn.sync()

    val listenEndpoint = serviceUrl
    val endpointValues = listenEndpoint.split(':')
    val http: Http = ignite()
            .ipAddress(endpointValues[0])
            .port(endpointValues[1].toInt())

    val service = UserTraversalService(redisConn)

    http.get("/start", function = {
        val startResult = service.Start()

        startResult.toString()
    })

    http.get("/progress") {
        val done = service.progress
        "$done"
    }
}