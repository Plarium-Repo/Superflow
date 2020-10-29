package com.plarium.south.superflow.core.spec.sink.kafka;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.BasePTupleRowSink;
import com.plarium.south.superflow.core.spec.sink.kafka.serde.KafkaBeamAvroSerializer;
import com.plarium.south.superflow.core.spec.sink.mapping.TopicSinkMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.stream.Collectors;


public abstract class BaseKafkaTupleSink extends BasePTupleRowSink {

    @Getter @NotNull
    @JsonPropertyDescription("Kafka bootstrap.servers: <host1:port,..,hostn:port>")
    protected final String bootstrap;

    @Getter @NotNull
    @JsonPropertyDescription("Kafka topic to dataset mapping")
    protected final MappingList<TopicSinkMapping> mapping;

    @Getter
    @JsonPropertyDescription("Kafka producer properties")
    protected final Map<String, Object> kafkaConfig;


    public BaseKafkaTupleSink(
            @Nullable String name,
            String bootstrap,
            @Nullable Boolean createSchema,
            @Nullable BaseRegistry registry,
            @Nullable BaseWindow window,
            MappingList<TopicSinkMapping> mapping,
            @Nullable Map<String, Object> kafkaConfig)
    {
        super(name, registry, createSchema, window);
        this.mapping = mapping;
        this.bootstrap = bootstrap;
        this.kafkaConfig = kafkaConfig;
    }

    protected abstract Class<? extends Serializer<Row>> getKafkaValueSerializer();

    @Override
    public void setup() {
        mapping.setup();
    }

    @Override
    public PDone expand(PCollectionTuple input) {
        for (TopicSinkMapping map: mapping.getList()) {
            String datasetName = map.getTag();
            if (!input.has(datasetName)) continue;

            PCollection<Row> dataset = input.get(datasetName);
            applyOutputPart(map.getTopic(), applyWindow(dataset));

            String schemaText = inferOrFetchSchema(map, dataset);
            Schema avroSchema = new Schema.Parser().parse(schemaText);
            createSchemaIfNeeded(map.getSchema(), avroSchema.toString());
        }

        return PDone.in(input.getPipeline());
    }

    protected KafkaIO.Write<byte[], Row> createWriter(String topic) {
        return KafkaIO.<byte[], Row>write()
                .withTopic(topic)
                .withBootstrapServers(bootstrap)
                .withProducerConfigUpdates(producerConfig())
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(getKafkaValueSerializer());
    }

    protected Map<String, Object> producerConfig() {
        Map<String, Object> config = Maps.newHashMap();
        config.put(KafkaBeamAvroSerializer.MAP_TOPIC_TO_SCHEMA_KEY, mapTopicToSchema());
        config.putAll(registry.serdeConfig());

        if(this.kafkaConfig != null) {
            config.putAll(kafkaConfig);
        }

        return config;
    }

    protected Map<String, String> mapTopicToSchema() {
        return mapping.getList().stream()
                .collect(Collectors.toMap(
                        TopicSinkMapping::getTopic,
                        TopicSinkMapping::getSchema));
    }

    protected void applyOutputPart(String topic, PCollection<Row> dataset) {
        dataset.apply(createWriter(topic).values());
    }
}
