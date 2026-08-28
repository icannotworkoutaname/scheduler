package com.chronos.scheduler.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.web.client.RestClient
import java.net.http.HttpClient
import java.time.Duration

@Configuration
class HttpSinkConfig {

    /**
     * requirements.md §7.5: sink timeout (10s) must be strictly below the lease
     * TTL (30s), so ordinary slow downstreams never cause a takeover. Both the
     * connection and the response-wait timeout are set here — a downstream that
     * accepts the TCP connection but never responds is just as dangerous as one
     * that's unreachable.
     */
    @Bean
    fun sinkRestClient(): RestClient {
        val httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build()

        val requestFactory = JdkClientHttpRequestFactory(httpClient).apply {
            setReadTimeout(Duration.ofSeconds(10))
        }

        return RestClient.builder()
            .requestFactory(requestFactory)
            .build()
    }
}
