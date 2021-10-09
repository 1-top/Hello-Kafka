package com.harrybro.hook

import com.harrybro.consumer.SimpleStringConsumer
import com.harrybro.consumer.getKafkaConsumerConfig

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val GROUP_ID = "test-group"

fun main() {
    val consumer = SimpleStringConsumer(getKafkaConsumerConfig(BOOTSTRAP_SERVER, GROUP_ID))
    Runtime.getRuntime().addShutdownHook(Thread(consumer::wakeup))
}