package org.nmiljkovic

import com.jdiazcano.cfg4k.loaders.PropertyConfigLoader
import com.jdiazcano.cfg4k.providers.ProxyConfigProvider
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.api.StatefulRedisConnection
import com.lambdaworks.redis.api.async.RedisAsyncCommands
import org.json.JSONArray
import org.json.JSONObject
import spark.kotlin.Http
import spark.kotlin.ignite
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.HashMap

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

fun getHttp(url: String): Http {
    val endpointValues = url.split(':')
    return ignite()
            .ipAddress(endpointValues[0])
            .port(endpointValues[1].toInt())
}

fun main(args: Array<String>) {
    val (serviceName, serviceUrl, globals) = getConfiguration()

    val http = getHttp(serviceUrl)

    val redisClient = RedisClient.create("redis://${globals["redis.url"]}/0")
    val redisConn = redisClient.connect()

    val service = PresentationService(redisConn)
    service.start()

    http.get("/") {
        output(service.graph)
    }

    http.get("/graph") {
        service.graph
    }

    http.get("/path/:lang") {
        val lang = params("lang")
        service.fetchPathFor(lang)
    }
}

class PresentationService(val redisConn: StatefulRedisConnection<String, String>) {
    lateinit var asyncRedis: RedisAsyncCommands<String, String>
    var minCountThreshold: Int = 0
    var edges: HashMap<EdgeKey, Int> = HashMap()
    var languages: Array<String>? = null
    var languageCounts: HashMap<String, Int> = HashMap()
    var statistics: LanguageStatistics = LanguageStatistics(0, 0, 0)
    val graph: JSONObject
        get() = constructGraph()

    data class LanguageStatistics(val maxCount: Int = 0, val minCount: Int = 0, val totalCount: Int = 0)
    data class EdgeKey(val from: String, val to: String)

    private fun fetchLanguages() {
        val keysFuture = asyncRedis.keys("repo:count:*").get(5, TimeUnit.SECONDS)
        languages = keysFuture.map {
            val regex = """repo:count:(.*)""".toRegex()
            val match = regex.matchEntire(it)!!.groups
            match[1]!!.value
        }.toTypedArray()
    }

    private fun fetchLanguageCounts() {
        if (languages != null && !languageCounts.isNotEmpty()) {
            languages!!.map {
                it to asyncRedis.get("repo:count:$it")
            }.forEach {
                languageCounts.put(it.first, it.second.get().toInt())
            }
        } else {

        }
    }

    private fun fetchStatistics() {
        val totalCount = asyncRedis.get("repo:total_count").get().toInt()
        val max = languageCounts.maxBy { it.value }?.value ?: 0
        val min = languageCounts.minBy { it.value }?.value ?: 0

        statistics = LanguageStatistics(totalCount, max, min)
    }

    private fun constructGraph(): JSONObject {
        fetchLanguages()
        fetchLanguageCounts()
        filterLowCountEntries()
        fetchStatistics()

        val responseObject = JSONObject()
        responseObject.put("nodes", createNodeArray())

        //fetchEdges()
        responseObject.put("edges", createEdgeArray())

        return responseObject
    }

    private fun fetchEdges() {
        val keysFuture = asyncRedis.keys("repo:edge:*").get()
        val fetchedEdges = keysFuture.map {
            val regex = """repo:edge:(.*):(.*)""".toRegex()
            val match = regex.matchEntire(it)!!.groups
            EdgeKey(match[1]!!.value, match[2]!!.value)
        }.filter {
            languageCounts[it.from]!! > minCountThreshold && languageCounts[it.to]!! > minCountThreshold
        }.map {
            it to asyncRedis.get("repo:edge:${it.from}:${it.to}")
        }.map {
            it.first to it.second.get().toInt()
        }.toMap()

        edges.putAll(fetchedEdges)
    }

    private fun filterLowCountEntries() {
        val sorted = languageCounts.values.sortedByDescending { it }
        minCountThreshold = sorted[sorted.size / 5]

        languages = languages!!.filter {
            languageCounts[it]!! > minCountThreshold
        }.toTypedArray()
    }

    private fun createNodeArray(): JSONArray {
        val sorted = languageCounts.values.sortedByDescending { it }
        val sortedLength = sorted.size

        val nodes = languages!!.mapIndexed { index, lang ->
                val node = JSONObject()
                val count = languageCounts[lang] ?: 0
                val rand = Random()
                node.put("id", lang)
                node.put("label", lang)

                for (i in 1..10) {
                    if (count < sorted[sortedLength / (5 * i)]) {
                        node.put("size", i + 10)
                        break
                    } else {
                        node.put("size", 20)
                    }
                }

                node.put("color", getRandomColor())
                node.put("x", rand.nextInt(100))
                node.put("y", rand.nextInt(100))
                node
        }
        return JSONArray(nodes)
    }

    private fun createEdgeArray(): JSONArray {
        var count = 0
        val edges = edges.map {
            val edge = JSONObject()
            edge.put("id", count++)
            edge.put("source", it.key.from)
            edge.put("target", it.key.to)
            edge.put("size", 2)
            edge
        }
        return JSONArray(edges)
    }

    fun start() {
        asyncRedis = redisConn.async()
    }


    fun fetchPathFor(lang: String): JSONArray {
        var count = 0

        val keysFuture = asyncRedis.keys("repo:path:$lang:*").get()
        val paths = keysFuture.map {
                it to asyncRedis.get(it)
            }.map {
                it.first to it.second.get()
            }.sortedByDescending {
                it.second
            }.take(5).map {
                it.first.split(":").drop(2).filter {
                    languageCounts[it]!! > minCountThreshold
                }.toEdges()
            }.flatten().onEach {
                it.put("id", count++)
            }

        return JSONArray(paths)
    }
}

fun getRandomColor() : String {
    val rand = Random()
    when (rand.nextInt(6)) {
        0 -> return "#0074D9"
        1 -> return "#39CCCC"
        2 -> return "#01FF70"
        3 -> return "#FF851B"
        4 -> return "#001f3f"
        else -> return "#85144b"
    }
}

fun List<String>.toEdges(): List<JSONObject> {
    val rand = Random()
    return (this.subList(0, this.size - 1) zip this.subList(1, this.size))
        .map {
            val edge = JSONObject()
            edge.put("source", it.first)
            edge.put("target", it.second)
            edge.put("color", getRandomColor())
            edge.put("size", 3.5)
            edge
        }
}
