/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Table;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static java.util.stream.Collectors.toList;

/**
 * Typed mapping for tracking the conversion semantics between a table's schema and an individual partition's. This may include reordering, renaming, and type coercions. This transformation will later be used by Page Sources to produces a consistent Page type from different partition schemas
 * <p>
 * This will still exist for unpartitioned tables, but will be an identity transformation. That simplifies consumer logic.
 */
public class TableToPartitionMappings
{
    private final Map<Integer, Integer> tableToPartitionMapping;
    private final Map<Integer, HiveType> coercion;

    @JsonCreator
    public TableToPartitionMappings(
            @JsonProperty("tableToPartitionMapping") Map<Integer, Integer> tableToPartitionMapping,
            @JsonProperty("coercion") Map<Integer, HiveType> coercion)
    {
        checkArgument(tableToPartitionMapping.size() == coercion.size());
        this.tableToPartitionMapping = tableToPartitionMapping;
        this.coercion = coercion;
    }

    // TODO: Consolidate with the column handles method, possibly
    public static TableToPartitionMappings createIdentityMapping(Table table)
    {
        ImmutableMap.Builder<Integer, Integer> identityPermutation = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, HiveType> identityCoercions = ImmutableMap.builder();
        List<Column> dataColumns = table.getDataColumns();
        for (int i = 0; i < dataColumns.size(); i++) {
            identityPermutation.put(i, i);
            identityCoercions.put(i, dataColumns.get(i).getType());
        }
        return new TableToPartitionMappings(identityPermutation.build(), identityCoercions.build());
    }

    public static TableToPartitionMappings createIdentityMapping(List<HiveColumnHandle> columns)
    {
        // Only regular handles go through Table <-> Partition mapping
        List<HiveColumnHandle> regularHandles = columns.stream().filter(x -> x.getColumnType().equals(REGULAR)).collect(toList());

        ImmutableMap.Builder<Integer, Integer> identityPermutation = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, HiveType> identityCoercions = ImmutableMap.builder();
        for (int i = 0; i < regularHandles.size(); i++) {
            checkState(regularHandles.get(i).getHiveColumnIndex() == i, "Column don't seem to be aligned");
            identityPermutation.put(i, i);
            identityCoercions.put(i, regularHandles.get(i).getHiveType());
        }
        return new TableToPartitionMappings(identityPermutation.build(), identityCoercions.build());
    }

    @JsonProperty
    public Map<Integer, Integer> getTableToPartitionMapping()
    {
        return tableToPartitionMapping;
    }

    @JsonProperty
    public Map<Integer, HiveType> getCoercion()
    {
        return coercion;
    }

    public TableToPartitionMap getMapping(int tableIndex)
    {
        return new TableToPartitionMap(tableIndex);
    }

    public String getPartitionName()
    {
        throw new UnsupportedOperationException();
    }

    public class TableToPartitionMap
    {
        private final int tableIndex;

        TableToPartitionMap(int tableIndex)
        {
            this.tableIndex = tableIndex;
        }

        public boolean isPresent()
        {
            return tableToPartitionMapping.containsKey(tableIndex);
        }

        public int getTableIndex()
        {
            return tableIndex;
        }

        public int getPartitionIndex()
        {
            return tableToPartitionMapping.get(tableIndex);
        }

        public HiveType getTableType()
        {
            throw new UnsupportedOperationException();
        }

        public HiveType getPartitionType()
        {
            return coercion.get(tableIndex);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableToPartitionMappings that = (TableToPartitionMappings) o;
        return Objects.equals(tableToPartitionMapping, that.tableToPartitionMapping) &&
                Objects.equals(coercion, that.coercion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableToPartitionMapping, coercion);
    }
}
