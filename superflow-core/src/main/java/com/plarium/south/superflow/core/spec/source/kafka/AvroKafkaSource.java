package com.plarium.south.superflow.core.spec.source.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.source.kafka.serde.KafkaBeamRowDeserializer;
import com.plarium.south.superflow.core.spec.source.mapping.TopicSourceAvroMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.common.serialization.Deserializer;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SOURCE;
import static com.plarium.south.superflow.core.spec.source.kafka.serde.KafkaBeamRowDeserializer.*;

@SchemaDefinition(
        type = SOURCE,
        baseTypes = {"registry", "window"},
        required = {"type", "offset", "groupId", "bootstrap", "mapping"})

@JsonPropertyOrder({"type", "name", "registry", "window", "allowNoStrict", "allowSchemaEvo",
        "offset", "groupId", "bootstrap", "autoCommit", "mapping", "kafkaConfig"})
public class AvroKafkaSource extends BaseKafkaSource<TopicSourceAvroMapping> {
    public static final String TYPE = "source/kafka/avro";

    @Getter @NotNull
    @JsonPropertyDescription("Allow process records with different schemas")
    private final Boolean allowNoStrict;

    @Getter @NotNull
    @JsonPropertyDescription("Allow schema evolution for kafka deserialization")
    private final Boolean allowSchemaEvo;

    @JsonCreator
    public AvroKafkaSource(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "groupId", required = true) String groupId,
            @JsonProperty(value = "bootstrap", required = true) String bootstrap,
            @JsonProperty(value = "offset", required = true) String offset,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "autoCommit") Boolean autoCommit,
            @JsonProperty(value = "delayCommit") String delayCommit,
            @JsonProperty(value = "mapping", required = true) MappingList<TopicSourceAvroMapping> mapping,
            @JsonProperty(value = "kafkaConfig") Map<String, Object> kafkaConfig,
            @JsonProperty(value = "allowNoStrict") Boolean allowNoStrict,
            @JsonProperty(value = "allowSchemaEvo") Boolean allowSchemaEvo)
    {
        super(name, groupId, bootstrap, offset, registry, window, autoCommit, delayCommit, mapping, kafkaConfig);
        this.allowNoStrict = allowNoStrict;
        this.allowSchemaEvo = allowSchemaEvo;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void setup() {
        super.setup();

        mapping.getList().forEach(m -> {
            if (m.autoCommitIsNeeded()) m.setAutoCommit(autoCommit);
            if (m.delayCommitIsNeeded()) m.setDelayCommit(delayCommit);
        });
    }

    @Override
    protected Schema getBeamSchema(TopicSourceAvroMapping map) {
        String avroSchema = map.fetchOrGetAvroSchema(registry);
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return AvroUtils.toBeamSchema(parser.parse(avroSchema));
    }

    @Override
    public Class<? extends Deserializer<Row>> valueDeserializer() {
        return KafkaBeamRowDeserializer.class;
    }

    @Override
    protected Map<String, Object> consumerConfig(TopicSourceAvroMapping map) {
        Map<String, Object> config = super.consumerConfig(map);

        String avroSchema = map.fetchOrGetAvroSchema(registry);
        config.put(AVRO_SCHEMA_STRING, avroSchema);

        if (allowNoStrict != null) config.put(ALLOW_NO_STRICT, allowNoStrict);
        if (allowSchemaEvo != null) config.put(ALLOW_SCHEMA_EVO, allowSchemaEvo);

        return config;
    }
}
