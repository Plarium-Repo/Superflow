package com.plarium.south.superflow.core.spec.dofn;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

public class BeamRowToBQRowFn extends DoFn<Row, TableRow> {

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<TableRow> out) {
        out.output(BigQueryUtils.toTableRow(row));
    }
}
