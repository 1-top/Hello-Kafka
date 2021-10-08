package com.harrybro.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import java.util.*

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val GROUP_ID = "test-group"

fun main() {
    SimpleStringASyncCommitConsumer(getKafkaOffsetCommitConsumerConfig(BOOTSTRAP_SERVER, GROUP_ID)).also {
        it.subscribe(mutableListOf(TOPIC_NAME))
        var consumerRecords: ConsumerRecords<String, String>
        while (true) {
            consumerRecords = it.poll()
            if (consumerRecords.count() != 0) {
                for (consumerRecord in consumerRecords) println(consumerRecord)
                it.commitAsync { offsets, e ->
                    if (e != null) println("Commit fail")
                    else println("Commit succeeded")
                    println(offsets)
                }
            }
        }
    }
}

class SimpleStringASyncCommitConsumer(config: Properties) : SimpleStringConsumer(config) {
    fun commitAsync(callback: OffsetCommitCallback) = super.consumer.commitAsync(callback)
}