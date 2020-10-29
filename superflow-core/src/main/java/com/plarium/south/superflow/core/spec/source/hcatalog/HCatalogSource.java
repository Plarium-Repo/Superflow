package com.plarium.south.superflow.core.spec.source.hcatalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.source.BasePTupleRowSource;
import com.plarium.south.superflow.core.spec.source.mapping.HCatSourceMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.beam.sdk.io.hcatalog.HCatToRow;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SOURCE;

@SchemaDefinition(
        type = SOURCE,
        baseTypes = {"window"},
        required = {"type", "uri", "mapping"})

@JsonPropertyOrder({"type", "uri", "name", "window", "config", "mapping"})
public class HCatalogSource extends BasePTupleRowSource {
    public static final String TYPE = "source/hcat";

    public static final String HIVE_METASTORE_URIS_KEY = "hive.metastore.uris";


    @Getter @NotNull
    @JsonPropertyDescription("The hcatalog metastore URI for connect to thrift server")
    private final String uri;

    @Getter @NotNull
    @JsonPropertyDescription("The additional configuration for hcatalog")
    private final Map<String, String> config;

    @Getter @NotEmpty
    @JsonPropertyDescription("The HCatalog mapping")
    private final HCatalogSourceMappingList mapping;

    @JsonCreator
    public HCatalogSource(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "uri", required = true) String uri,
            @JsonProperty(value = "config") Map<String, String> config,
            @JsonProperty(value = "mapping", required = true) HCatalogSourceMappingList mapping)
    {
        super(name, null, window);
        this.uri = uri;
        this.mapping = mapping;
        this.config = firstNonNull(config, Maps.newHashMap());
    }

    @Override
    public void setup() {
        mapping.setup();
        config.put("hive.metastore.uris", uri);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean registryIsNeed() {
        return false;
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
        PCollectionTuple tuple = PCollectionTuple.empty(input.getPipeline());

        for (HCatSourceMapping map : mapping.getList()) {
            PCollection<Row> dataset = expandPart(input, map);
            dataset = applyWindow(dataset);
            dataset = applyTimePolicy(dataset, map.getEventTime());
            tuple = tuple.and(map.getTag(), dataset);
        }

        return tuple;
    }

    private PCollection<Row> expandPart(PBegin input, HCatSourceMapping map) {
        HCatalogIO.Read reader = createReader(map);
        return HCatToRow.fromSpec(reader).expand(input);
    }

    private HCatalogIO.Read createReader(HCatSourceMapping map) {
         return HCatalogIO.read()
                .withTable(map.getTable())
                .withFilter(map.getFilter())
                .withConfigProperties(config)
                .withDatabase(map.getDatabase())
                .withPartitionCols(map.getPartitions());
    }
}
