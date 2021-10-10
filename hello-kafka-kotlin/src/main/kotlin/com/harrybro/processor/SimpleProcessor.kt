package com.harrybro.processor

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig.*
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.Processor
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.ProcessorSupplier
import java.util.*

class FilterProcessor : Processor<String, String> {
    private var context: ProcessorContext? = null

    override fun init(context: ProcessorContext?) {
        this.context = context
    }

    override fun process(key: String?, value: String) {
        if (value.length > 5) context?.forward(key, value)
        context?.commit()
    }

    override fun close() {}
}

private const val APPLICATION_NAME = "processor-application"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val STREAM_LOG = "stream_log"
private const val STREAM_LOG_FILTER = "stream_log_filter"

fun main() {
    Topology().apply {
        addSource("log-source", STREAM_LOG)
        addProcessor("filter-stream", ProcessorSupplier { FilterProcessor() }, "log-source")
        addSink("sink", STREAM_LOG_FILTER, "filter-stream")
    }.let {
        KafkaStreams(it, getProperties())
    }.also {
        it.start()
    }
}

private fun getProperties() = Properties().apply {
    put(APPLICATION_ID_CONFIG, APPLICATION_NAME)
    put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
    put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
    put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
}