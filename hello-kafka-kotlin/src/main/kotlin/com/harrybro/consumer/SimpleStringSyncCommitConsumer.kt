package com.harrybro.consumer

import java.util.*

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val GROUP_ID = "test-group"

fun main() {
    SimpleStringSyncCommitConsumer(getKafkaOffsetCommitConsumerConfig(BOOTSTRAP_SERVER, GROUP_ID)).also {
        it.subscribe(mutableListOf(TOPIC_NAME))
        while (true) {
            for (consumerRecord in it.poll()) println(consumerRecord)
            it.commitSync()
        }
    }
}

class SimpleStringSyncCommitConsumer(config: Properties) : SimpleStringConsumer(config) {
    fun commitSync() = super.consumer.commitSync()
}