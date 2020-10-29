package com.plarium.south.superflow.core.spec.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class BQRowToBeamRowFn extends DoFn<TableRow, Row> {

    private final Schema beamSchema;

    public BQRowToBeamRowFn(Schema beamSchema) {
        this.beamSchema = beamSchema;
    }

    @ProcessElement
    public void process(@Element TableRow bqRow, OutputReceiver<Row> out) {
        out.output(BigQueryUtils.toBeamRow(beamSchema, bqRow));
    }
}
