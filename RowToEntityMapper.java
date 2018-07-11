package com.gfs.data.pipeline.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;

public class RowToEntityMapper implements JdbcIO.RowMapper<Entity> {
    private static final long serialVersionUID = 1L;
    static final Logger LOG = LoggerFactory.getLogger(RowToEntityMapper.class);

    private final String keyColumnName;
    private final IncompleteKey partialKey;

    public RowToEntityMapper(final String keyColumnName, final IncompleteKey partialKey) {
        this.keyColumnName = keyColumnName;
        this.partialKey = partialKey;
    }

    @Override
    public Entity mapRow(final ResultSet resultSet) throws Exception {
        // Create a keyless entity. Can't persist this to CDS, but need an Entity (not a builder) so
        // we can extract the value of the key column.
        final FullEntity<IncompleteKey> keylessEntity = populateEntity(resultSet);
        final String keyValue = keylessEntity.getString(keyColumnName);

        // Now, add the extracted key value to the rest of the key, which includes the project ID
        // and Kind at a minimum.
        final Key completeKey = Key.newBuilder(partialKey, keyValue).build();

        LOG.info("key={}", completeKey);

        // finally, add the key to the entity and return.
        return Entity.newBuilder(completeKey, keylessEntity).build();
    }

    protected FullEntity<IncompleteKey> populateEntity(final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int colCount = metaData.getColumnCount();
        LOG.info("Retrieved columns={}", colCount);
        FullEntity.Builder<IncompleteKey> builder = FullEntity.newBuilder();

        for (int colIndex = colCount; colIndex > 0; colIndex--) {
            builder = populateProperty(resultSet, metaData, builder, colIndex);
        }
        return builder.build();
    }

    protected FullEntity.Builder<IncompleteKey> populateProperty(final ResultSet resultSet,
                                                                 final ResultSetMetaData metaData,
                                                                 final FullEntity.Builder<IncompleteKey> builder,
                                                                 final int colIndex) throws SQLException {
        final String colName = metaData.getColumnLabel(colIndex);

        try {

            switch (metaData.getColumnType(colIndex)) {
            // case java.sql.Types.ARRAY:
            // obj.put(colIndex, resultSet.getArray(colIndex));
            // break;

            case java.sql.Types.BIGINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
                final long longVal = resultSet.getLong(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName) : builder.set(colName, longVal);

            case java.sql.Types.BOOLEAN:
                final boolean boolVal = resultSet.getBoolean(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName) : builder.set(colName, boolVal);

            case java.sql.Types.BLOB:
                final java.sql.Blob blob = resultSet.getBlob(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName)
                        : builder.set(colName,
                            com.google.cloud.datastore.Blob.copyFrom(blob.getBytes(0L, (int) blob.length())));

            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
                final double doubleVal = resultSet.getDouble(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName) : builder.set(colName, doubleVal);

            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                final BigDecimal bigDecVal = resultSet.getBigDecimal(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName)
                        : builder.set(colName, bigDecVal.doubleValue());

            case java.sql.Types.NCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
                String stringVal = resultSet.getNString(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName) : builder.set(colName, stringVal);

            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
                stringVal = resultSet.getString(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName) : builder.set(colName, stringVal);

            case java.sql.Types.DATE:
                final java.sql.Date date = resultSet.getDate(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName)
                        : builder.set(colName,
                            com.google.cloud.Timestamp.of(new java.util.Date(date.getTime())));

            case java.sql.Types.TIMESTAMP:
                final java.sql.Timestamp timestamp = resultSet.getTimestamp(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName)
                        : builder.set(colName, com.google.cloud.Timestamp.of(timestamp));

            default:
                final Object object = resultSet.getObject(colIndex);
                return resultSet.wasNull() ? builder.setNull(colName)
                        : builder.set(colName, object.toString());
            }
        } catch (final SQLException e) {
            LOG.error(String.format("Problem converting column %s with type %s",
                colName,
                metaData.getColumnTypeName(colIndex)), e);
        }

        return builder;
    }
}
