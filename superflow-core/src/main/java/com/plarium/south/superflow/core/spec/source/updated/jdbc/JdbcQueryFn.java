package com.plarium.south.superflow.core.spec.source.updated.jdbc;

import com.plarium.south.superflow.core.spec.commons.jdbc.DataSourceConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


@Slf4j
public class JdbcQueryFn extends DoFn<Long, Row> {

    private final String query;

    private final DataSourceConfig dataSourceConfig;

    private final JdbcIO.RowMapper<Row> jdbcRowMapper;


    public JdbcQueryFn(
            String query,
            DataSourceConfig dataSourceConfig,
            JdbcIO.RowMapper<Row> jdbcRowMapper)
    {
        this.query = query;
        this.dataSourceConfig = dataSourceConfig;
        this.jdbcRowMapper = jdbcRowMapper;
    }


    @DoFn.ProcessElement
    public void process(OutputReceiver<Row> out) throws Exception {
        try (BasicDataSource dataSource = dataSourceConfig.buildDataSource())
        {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(query);
                 ResultSet resultSet = statement.executeQuery())
            {
                if (!resultSet.isBeforeFirst()) {
                    log.warn("Sql return empty result for query: {}", statement);
                    return;
                }

                while (resultSet.next()) {
                    out.output(jdbcRowMapper.mapRow(resultSet));
                }
            }
        }
    }
}
