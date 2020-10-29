package com.plarium.south.superflow.core.spec.processor.enrich.jdbc;

import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import com.plarium.south.superflow.core.spec.commons.jdbc.RowStatementPreparer;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.plarium.south.superflow.core.utils.BeamRowUtils.mergeRows;

@Slf4j
public class ReadJdbcFn extends DoFn<Row, Row> {

    private final String query;

    private final Schema mergeSchema;

    private final DataSourceConfig dataSourceConfig;

    private final JdbcIO.RowMapper<Row> jdbcRowMapper;

    private final JdbcIO.PreparedStatementSetter<Row> statementPreparer;


    private transient Connection connection;

    private transient BasicDataSource dataSource;


    public ReadJdbcFn(
            String query,
            Schema mergeSchema,
            DataSourceConfig dataSourceConfig,
            JdbcIO.RowMapper<Row> jdbcRowMapper,
            RowStatementPreparer statementPreparer)
    {
        this.query = query;
        this.mergeSchema = mergeSchema;
        this.dataSourceConfig = dataSourceConfig;
        this.jdbcRowMapper = jdbcRowMapper;
        this.statementPreparer = statementPreparer;
    }

    @Setup
    public void setup() throws Exception {
        dataSource = dataSourceConfig.buildDataSource();
        connection = dataSource.getConnection();
    }

    @ProcessElement
    public void processElement(@Element Row originRow, ProcessContext context) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statementPreparer.setParameters(originRow, statement);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.isBeforeFirst()) {
                    log.warn("Sql return empty result for query: {}", statement);
                    return;
                }

                while (resultSet.next()) {
                    Row enrichRow = jdbcRowMapper.mapRow(resultSet);
                    Row mergeRow = mergeRows(mergeSchema, originRow, enrichRow);
                    context.output(mergeRow);
                }
            }
        }
    }

    @Teardown
    public void teardown() throws Exception {
        connection.close();
        dataSource.close();
    }
}
