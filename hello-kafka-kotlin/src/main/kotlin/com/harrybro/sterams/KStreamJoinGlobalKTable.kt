package com.harrybro.sterams

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.*

private const val APPLICATION_NAME = "global-table-join-application"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val ADDRESS_TABLE = "address_v3"
private const val ORDER_STREAM = "order"
private const val ORDER_JOIN_STREAM = "order_join"

fun main() {
    val streamsBuilder = StreamsBuilder().apply {
        stream<String, String>(ORDER_STREAM)
            .join(globalTable<String, String>(ADDRESS_TABLE),
                { orderKey, orderValue -> orderKey },
                { order, address -> "$order send to $address" })
            .to(ORDER_JOIN_STREAM)
    }
    KafkaStreams(streamsBuilder.build(), getProperties()).start()
}

private fun getProperties() = Properties().apply {
    put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME)
    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
    put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
}