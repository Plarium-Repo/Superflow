package com.plarium.south.superflow.core.spec.source.hcatalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.TestSpecUtils;
import com.plarium.south.superflow.core.spec.source.mapping.HCatSourceMapping;
import com.plarium.south.superflow.core.utils.ParseUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.plarium.south.superflow.core.spec.source.hcatalog.HCatalogSource.HIVE_METASTORE_URIS_KEY;
import static org.junit.Assert.*;

public class HCatalogSourceTest {

    private String sourceName = "hcat-source";
    private String hiveMetastoreUris = "thrift://host:port";
    private String hiveDatabase = "default";
    private String outputDataset = "hive_result";
    private String hiveTable = "table";
    private String hiveFiler = "date=yyy-mm-dd";
    private String partitionColumn = "date";
    private String eventTime = "timestamp";


    protected Map<String, String> getEnv() {
        HashMap<String, String> env = Maps.newHashMap();
        env.put("source.name", sourceName);
        env.put("hive.metastore.uris", hiveMetastoreUris);
        env.put("hive.database", hiveDatabase);
        env.put("output.dataset", outputDataset);
        env.put("output.table", hiveTable);
        env.put("output.filer", hiveFiler);
        env.put("event.time.field", eventTime);
        env.put("partition.column", partitionColumn);
        return env;
    }


    @Test
    public void testSpecYamlParsing() throws IOException {
        String path = "/templates/sources/hcatalog/hcatalog-source.yaml";
        String yamlTemplated = TestSpecUtils.renderTemplate(path, getEnv());
        TypeReference<HCatalogSource> type = new TypeReference<HCatalogSource>() {};
        List<HCatalogSource> sources = ParseUtils.parseYaml(yamlTemplated.getBytes(), type);

        for (HCatalogSource source : sources) {
            source.setup();

            assertEquals(source.getName(), sourceName);
            assertEquals(source.getUri(), hiveMetastoreUris);

            Map<String, String> config = source.getConfig();
            assertNotNull(config);
            assertFalse(config.isEmpty());
            assertNotNull(config.get(HIVE_METASTORE_URIS_KEY));

            HCatalogSourceMappingList mapping = source.getMapping();
            assertNotNull(mapping);
            assertFalse(mapping.getList().isEmpty());

            HCatSourceMapping map = mapping.getList().get(0);
            assertEquals(map.getDatabase(), hiveDatabase);
            assertEquals(map.getTable(), hiveTable);
            assertEquals(map.getFilter(), hiveFiler);
            assertEquals(map.getTag(), outputDataset);
            assertEquals(map.getEventTime(), eventTime);

            List<String> partitions = map.getPartitions();
            assertNotNull(partitions);
            assertFalse(partitions.isEmpty());
            assertEquals(partitions.get(0), partitionColumn);
        }
    }
}