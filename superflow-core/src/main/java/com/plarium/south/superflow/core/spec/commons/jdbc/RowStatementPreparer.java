package com.plarium.south.superflow.core.spec.commons.jdbc;

import com.plarium.south.superflow.core.spec.commons.jdbc.StatementMapping.VariableType;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class RowStatementPreparer implements JdbcIO.PreparedStatementSetter<Row> {

    private final List<StatementMapping> variables;


    public RowStatementPreparer(List<StatementMapping> variables) {
        this.variables = variables;
    }


    @Override
    public void setParameters(Row row, PreparedStatement statement) {
        variables.forEach(val -> applyVariable(row, val, statement));
    }

    @SuppressWarnings("ConstantConditions")
    private void applyVariable(Row row, StatementMapping variable, PreparedStatement statement) {
        String name = variable.getName();
        Integer order = variable.getOrder();
        VariableType type = variable.getType();

        try {
            switch (type) {
                case SHORT:
                    statement.setShort(order, row.getInt16(name));
                    return;
                case INT:
                    statement.setInt(order, row.getInt32(name));
                    return;
                case LONG:
                    statement.setLong(order, row.getInt64(name));
                    return;
                case DECIMAL:
                    statement.setBigDecimal(order, row.getDecimal(name));
                    return;
                case FLOAT:
                    statement.setFloat(order, row.getFloat(name));
                    return;
                case DOUBLE:
                    statement.setDouble(order, row.getDouble(name));
                    return;
                case STRING:
                    statement.setString(order, row.getString(name));
                    return;
                case BOOL:
                    statement.setBoolean(order, row.getBoolean(name));
                    return;
                default:
                    throw new IllegalStateException("Unexpected value: " + type);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
