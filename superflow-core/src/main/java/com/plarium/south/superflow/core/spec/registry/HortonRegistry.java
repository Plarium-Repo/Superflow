package com.plarium.south.superflow.core.spec.registry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Maps;
import com.hortonworks.registries.schemaregistry.*;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.utils.HortonRegistryUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.REGISTRY;


@Slf4j

@SchemaDefinition(
        type = REGISTRY,
        ignoreFields = {"window", "registry"},
        required = {"type", "url"})

@JsonPropertyOrder({"type", "url", "schemaGroup", "compatibility", "versions"})
public class HortonRegistry extends BaseRegistry {
    public static final String TYPE = "registry/horton";

    private static final String SCHEMA_DESC_TEMPLATE =
            "Schema registered by superflow, from AvroFileStoreSink for schemaName: [%s]";


    @Getter @NotNull
    @JsonPropertyDescription("Avro schema group name")
    private final String schemaGroup;

    @Getter @NotNull
    @JsonPropertyDescription("Avro schema compatibility: backward (default), forward, both, none")
    protected final SchemaCompatibility compatibility;

    @Getter @NotNull
    @JsonPropertyDescription("Avro schema versions for reader: <name:version>")
    private final Map<String, Integer> versions;

    @JsonIgnore
    private transient SchemaRegistryClient registryClient;


    public HortonRegistry(
            @JsonProperty(value = "schemaGroup") String schemaGroup,
            @JsonProperty(value = "compatibility") String compatibility,
            @JsonProperty(value = "versions") Map<String, Integer> versions,
            @JsonProperty(value = "url", required = true) String url)
    {
        super(url);
        this.versions = versions;
        this.schemaGroup = schemaGroup;
        this.compatibility = compatibilityToEnum(compatibility);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public Map<String, Object> serdeConfig() {
        Map<String, Object> config = Maps.newHashMap();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), url);

        if (versions != null) config.put(KafkaAvroDeserializer.READER_VERSIONS, versions);
        if (schemaGroup != null) config.put(KafkaAvroSerializer.SCHEMA_GROUP, schemaGroup);
        if (compatibility != null) config.put(KafkaAvroSerializer.SCHEMA_COMPATIBILITY, compatibility);

        return config;
    }

    private SchemaCompatibility compatibilityToEnum(String compatibility) {
        if (compatibility == null) return SchemaCompatibility.DEFAULT_COMPATIBILITY;
        return SchemaCompatibility.valueOf(compatibility.toUpperCase());
    }

    @Override
    public boolean hasSchema(String schemaName) {
        return HortonRegistryUtils.hasSchema(getClient(), schemaName);
    }

    @Override
    public String getSchema(final String schemaName) {
        return findVersion(schemaName)
                .map(v -> HortonRegistryUtils.getSchemaInfo(getClient(), schemaName, v))
                .orElse(HortonRegistryUtils.getLatestSchemaInfo(getClient(), schemaName))
                .getSchemaText();
    }

    @Override
    public Map<Integer, String> getAllVersion(String schemaName) {
        return HortonRegistryUtils
                .getAllSchemaInfo(getClient(), schemaName)
                .stream().collect(Collectors.toMap(
                        SchemaVersionInfo::getVersion,
                        SchemaVersionInfo::getSchemaText));
    }

    @Override
    public void createSchema(String schemaName, String schemaString) {
        final SchemaIdVersion idVersion;
        SchemaMetadata metadata = createSchemaMetadata(schemaName);
        SchemaVersion version = new SchemaVersion(schemaString, metadata.getDescription());

        try {
            idVersion = getClient().addSchemaVersion(metadata, version);
            log.info("The successfully registered schema: {}", idVersion);
        }
        catch (InvalidSchemaException | IncompatibleSchemaException | SchemaNotFoundException e) {
            String msg = "Failed create/update avro schema: \n" + metadata;
            throw new IllegalStateException(msg, e);
        }
    }


    private SchemaRegistryClient getClient() {
        if(registryClient == null) {
            registryClient = HortonRegistryUtils.createClient(url);
        }

        return registryClient;
    }

    private Optional<Integer> findVersion(String schemaName) {
        if (versions == null || !versions.containsKey(schemaName)) {
            return Optional.empty();
        }

        return Optional.of(versions.get(schemaName));
    }

    private SchemaMetadata createSchemaMetadata(String schemaName) {
        return new SchemaMetadata.Builder(schemaName)
                .type("avro")
                .schemaGroup(schemaGroup)
                .compatibility(compatibility)
                .description(String.format(SCHEMA_DESC_TEMPLATE, schemaName))
                .build();
    }
}
