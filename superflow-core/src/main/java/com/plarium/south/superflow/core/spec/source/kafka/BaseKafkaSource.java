package com.plarium.south.superflow.core.spec.source.kafka;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.source.BasePTupleRowSource;
import com.plarium.south.superflow.core.spec.source.mapping.TopicSourceMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import com.plarium.south.superflow.core.utils.DurationUtils;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.joda.time.Duration;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public abstract class BaseKafkaSource<T extends TopicSourceMapping> extends BasePTupleRowSource {

    public static final String DEFAULT_DELAY_COMMIT = "5 sec";

    @Getter @NotNull
    @JsonPropertyDescription("Kafka group.id")
    protected final String groupId;

    @Getter @NotNull
    @JsonPropertyDescription("Kafka bootstrap.servers: <host1:port,..,hostn:port>")
    protected final String bootstrap;

    @Getter @NotNull
    @JsonPropertyDescription("Kafka auto.offset.reset: <earliest|latest|none>")
    protected final String offset;

    @Getter @NotNull
    @JsonPropertyDescription("The enable auto commit kafka offset, default: true")
    protected final Boolean autoCommit;

    @Getter @NotNull
    @JsonPropertyDescription("Delay between offset commits, default: '5 sec' (working if option autoCommit: true)")
    protected final String delayCommit;

    @Getter @NotNull
    @JsonPropertyDescription("Kafka topic to dataset mapping")
    protected final MappingList<T> mapping;

    @Getter @NotNull
    @JsonPropertyDescription("Kafka consumer properties map<string, object>")
    protected final Map<String, Object> kafkaConfig;


    protected BaseKafkaSource(
            String name,
            String groupId,
            String bootstrap,
            String offset,
            BaseRegistry registry,
            BaseWindow window,
            Boolean autoCommit,
            String delayCommit,
            MappingList<T> mapping,
            Map<String, Object> kafkaConfig)
    {
        super(name, registry, window);
        this.groupId = groupId;
        this.bootstrap = bootstrap;
        this.offset = offset;
        this.autoCommit = firstNonNull(autoCommit, true);
        this.delayCommit = firstNonNull(delayCommit, DEFAULT_DELAY_COMMIT);
        this.mapping = mapping;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void setup() {
        mapping.setup();
    }

    protected abstract Schema getBeamSchema(T map);

    protected abstract Class<? extends Deserializer<Row>> valueDeserializer();


    @Override
    public PCollectionTuple expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        PCollectionTuple tuple = PCollectionTuple.empty(pipeline);

        for (T map : mapping.getList()) {
            Schema beamSchema = getBeamSchema(map);
            KafkaIO.Read<byte[], Row> reader = createReader(map);
            reader = applyTimePolicy(reader, map);

            PCollection<Row> dataset = applyOutputPart(pipeline, reader, beamSchema);
            tuple = tuple.and(map.getTag(), applyWindow(dataset));
        }

        return tuple;
    }

    protected Map<String, Object> consumerConfig(T map) {
        Map<String, Object> config = Maps.newHashMap();
        config.putAll(registry.serdeConfig());

        config.put(GROUP_ID_CONFIG, groupId);
        config.put(AUTO_OFFSET_RESET_CONFIG, offset);

        if(this.kafkaConfig != null) {
            config.putAll(this.kafkaConfig);
        }

        return config;
    }

    protected KafkaIO.Read<byte[], Row> createReader(T map) {
        Map<String, Object> config = consumerConfig(map);

        if(map.getAutoCommit()) {
            config.put(ENABLE_AUTO_COMMIT_CONFIG, true);

            Duration delay = DurationUtils.parse(map.getDelayCommit());
            config.put(AUTO_COMMIT_INTERVAL_MS_CONFIG, delay.getMillis());
        }

        return KafkaIO.<byte[], Row>read()
                .withTopic(map.getTopic())
                .withBootstrapServers(bootstrap)
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(valueDeserializer())
                .withConsumerConfigUpdates(config);
    }

    private PCollection<Row> applyOutputPart(Pipeline pipeline, KafkaIO.Read<byte[], Row> reader, Schema beamSchema) {
        return pipeline.apply(reader)
                .apply(MapElements.into(TypeDescriptor.of(Row.class))
                        .via(kv -> kv.getKV().getValue()))
                .setRowSchema(beamSchema);
    }

    private KafkaIO.Read<byte[], Row> applyTimePolicy(KafkaIO.Read<byte[], Row> reader, T mapping) {
        if (mapping.hasEventTime()) {
            return reader.withTimestampPolicyFactory((TimestampPolicyFactory<byte[], Row>)
                    (tp, previousWatermark) -> new KafkaEventTimePolicy(mapping.getEventTime(), previousWatermark));
        }

        return reader;
    }
}
