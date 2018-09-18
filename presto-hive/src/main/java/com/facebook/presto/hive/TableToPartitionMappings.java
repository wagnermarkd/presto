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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Typd mapping for tracking the conversion semantics between a table's schema and an individual partition's. This may include reordering, renaming, and type coercions. This transformation will later be used by Page Sources to produces a consistent Page type from different partition schemas
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
        this.tableToPartitionMapping = tableToPartitionMapping;
        this.coercion = coercion;
    }

    public static TableToPartitionMappings createIdentityMapping(Table table)
    {
        ImmutableMap.Builder<Integer, Integer> identityPermutation = ImmutableMap.builder();
        List<Column> dataColumns = table.getDataColumns();
        for (int i = 0; i < dataColumns.size(); i++) {
            identityPermutation.put(i, i);
        }
        return new TableToPartitionMappings(identityPermutation.build(), ImmutableMap.of());
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

    // TODO remove what I ended up not using
    public class TableToPartitionMap
    {
        private final int tableIndex;

        TableToPartitionMap(int tableIndex)
        {
            this.tableIndex = tableIndex;
        }

        public int getTableIndex()
        {
            return tableIndex;
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isPresent()
        {
            return tableToPartitionMapping.containsKey(tableIndex);
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
