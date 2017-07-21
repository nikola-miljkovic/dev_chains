package org.nmiljkovic

import com.lambdaworks.redis.api.StatefulRedisConnection
import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import java.util.HashMap

class RepositoryAnalysisService(redisConn: StatefulRedisConnection<String, String>, val rabbitChan: Channel) {
    private val exchangeName = "user_exchange"
    private val queueName = "user_queue"
    private val routingKey = "black"

    val redisAsync = redisConn.async()!!
    var status = ServiceStatus.Inactive

    companion object {
        val logger = LoggerFactory.getLogger(RepositoryAnalysisService::class.java)!!
    }

    private fun startService() {
        rabbitChan.exchangeDeclare(exchangeName, "direct", true)
        rabbitChan.queueDeclare(queueName, true, false, false, null)
        rabbitChan.queueBind(queueName, exchangeName, routingKey)
        rabbitChan.basicConsume(queueName, false, UserConsumer(rabbitChan, redisAsync))
        rabbitChan.basicQos(10)

        logger.info("Service started.")
        status = ServiceStatus.Running
    }

    fun start() {
        if (status == ServiceStatus.Inactive) {
            startService()
        }
    }
}

enum class ServiceStatus {
    Inactive,

    Running,
}