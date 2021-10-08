package com.harrybro.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*

fun getKafkaConsumerConfig(bootstrapServer: String, groupId: String) = Properties().apply {
    put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    put(GROUP_ID_CONFIG, groupId)
    put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
    put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
}

fun getKafkaOffsetCommitConsumerConfig(bootstrapServer: String, groupId: String) =
    getKafkaConsumerConfig(bootstrapServer, groupId).apply { put(ENABLE_AUTO_COMMIT_CONFIG, false) }