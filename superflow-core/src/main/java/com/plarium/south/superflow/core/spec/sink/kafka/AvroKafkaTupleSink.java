package com.plarium.south.superflow.core.spec.sink.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.kafka.serde.KafkaBeamRowSerializer;
import com.plarium.south.superflow.core.spec.sink.mapping.TopicSinkMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SINK;

@SchemaDefinition(
        type = SINK,
        baseTypes = {"registry", "window"},
        required = {"type", "bootstrap", "mapping"})

@JsonPropertyOrder({"type", "name", "registry", "window", "bootstrap", "createSchema", "mapping", "kafkaConfig"})
        public class AvroKafkaTupleSink extends BaseKafkaTupleSink {
    public static final String TYPE = "sink/kafka/avro";

    public AvroKafkaTupleSink(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "createSchema") Boolean createSchema,
            @JsonProperty(value = "kafkaConfig") Map<String, Object> kafkaConfig,
            @JsonProperty(value = "bootstrap", required = true) String bootstrap,
            @JsonProperty(value = "mapping", required = true) MappingList<TopicSinkMapping> mapping)
    {
        super(name, bootstrap, createSchema, registry, window, mapping, kafkaConfig);
    }

    @Override
    protected Class<? extends Serializer<Row>> getKafkaValueSerializer() {
        return KafkaBeamRowSerializer.class;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
