package com.harrybro.sterams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.*
import java.util.*

private const val APPLICATION_NAME = "streams-filter-application"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val STREAM_LOG = "stream_log"
private const val STREAM_LOG_FILTER = "stream_log_filter"

fun main() {
//    val streamsBuilder = StreamsBuilder()
//    val streamLog = streamsBuilder.stream<String, String>(STREAM_LOG) // source processor
//    val filterStreamLog = streamLog.filter { _, value -> value.length > 5 } // stream processor
//    filterStreamLog.to(STREAM_LOG_FILTER) // sink processor

    val streamsBuilder = StreamsBuilder().apply {
        stream<String, String>(STREAM_LOG)
            .filter { _, value -> value.length > 5 }
            .to(STREAM_LOG_FILTER)
    }

    val streams = KafkaStreams(streamsBuilder.build(), getProperties())
    streams.start()
}

private fun getProperties() = Properties().apply {
    put(APPLICATION_ID_CONFIG, APPLICATION_NAME)
    put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
    put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
}