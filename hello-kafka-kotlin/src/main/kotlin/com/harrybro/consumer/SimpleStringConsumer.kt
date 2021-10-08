package com.harrybro.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val GROUP_ID = "test-group"

fun main() {
    SimpleStringConsumer(getKafkaConsumerConfig(BOOTSTRAP_SERVER, GROUP_ID)).also {
        it.subscribe(mutableListOf(TOPIC_NAME))
        while (true) {
            for (consumerRecord in it.poll()) println(consumerRecord)
        }
    }
}

open class SimpleStringConsumer(private val config: Properties) {
    protected val consumer = KafkaConsumer<String, String>(this.config)
    fun subscribe(topicName: Collection<String>) = this.consumer.subscribe(topicName)
    fun poll(): ConsumerRecords<String, String> = this.consumer.poll(Duration.ZERO)
}