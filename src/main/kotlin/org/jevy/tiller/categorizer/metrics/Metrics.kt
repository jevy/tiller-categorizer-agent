package org.jevy.tiller.categorizer.metrics

import com.sun.net.httpserver.HttpServer
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.Executors

object Metrics {
    private val logger = LoggerFactory.getLogger(Metrics::class.java)
    val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    fun startHttpServer(port: Int) {
        val server = HttpServer.create(InetSocketAddress(port), 0)
        server.createContext("/metrics") { exchange ->
            val response = registry.scrape().toByteArray(Charsets.UTF_8)
            exchange.responseHeaders.set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            exchange.sendResponseHeaders(200, response.size.toLong())
            exchange.responseBody.use { it.write(response) }
        }
        server.executor = Executors.newSingleThreadExecutor()
        server.start()
        logger.info("Metrics server started on :{}/metrics", port)
    }

    fun pushToGateway(url: String, job: String) {
        val body = registry.scrape().toRequestBody("text/plain; version=0.0.4".toMediaType())
        val request = Request.Builder()
            .url("$url/metrics/job/$job")
            .put(body)
            .build()
        OkHttpClient().newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                logger.warn("Pushgateway push failed: {} {}", response.code, response.message)
            } else {
                logger.info("Pushed metrics to Pushgateway job={}", job)
            }
        }
    }
}
