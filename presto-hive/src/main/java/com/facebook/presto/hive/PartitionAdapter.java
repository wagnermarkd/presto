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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.charPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.floatPartitionKey;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.isHiveNull;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.hive.coercer.Coercers.createCoercer;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class PartitionAdapter
{
    private final Optional<BucketAdapter> bucketAdapter;
    private final Optional<OrderAdapter> orderAdapter;
    private final Optional<TypeAdapter> typeAdapter;
    private final Optional<PartitionValueAdapter> partitionValueAdapter;
    // TODO: get rid of this
    private final Type[] types;

    PartitionAdapter(List<HivePageSourceProvider.ColumnMapping> columnMappings, Map<Integer, Integer> materializedTableToPartitionMap, Optional<HivePageSourceProvider.BucketAdaptation> bucketAdapter, DateTimeZone hiveStorageTimeZone, TypeManager typeManager)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(materializedTableToPartitionMap, "materializedTableToPartitionMap is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");
        this.bucketAdapter = bucketAdapter.map(BucketAdapter::new);

        // Prepare order
        int size = columnMappings.size();
        if (!isIdentity(materializedTableToPartitionMap, size)) {
            orderAdapter = Optional.of(new OrderAdapter(materializedTableToPartitionMap, size));
        }
        else {
            orderAdapter = Optional.empty();
        }

        // Prepare types
        types = new Type[size];
        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            HivePageSourceProvider.ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;
        }

        // Prepare type adaptation
        ImmutableMap.Builder<Integer, Function<Block, Block>> coercersBuilder = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            HivePageSourceProvider.ColumnMapping columnMapping = columnMappings.get(columnIndex);
            if (columnMapping.getCoercionFrom().isPresent()) {
                coercersBuilder.put(columnIndex, createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType()));
            }
        }
        ImmutableMap<Integer, Function<Block, Block>> coercers = coercersBuilder.build();
        if (coercers.isEmpty()) {
            this.typeAdapter = Optional.empty();
        }
        else {
            TypeAdaptation typeAdaptation = new TypeAdaptation(coercers);
            this.typeAdapter = Optional.of(new TypeAdapter(typeAdaptation, size));
        }

        // TODO Move to method
        // Prepare partition value adaptation
        ImmutableMap.Builder<Integer, PrefilledValue> prefilledValueBuilder = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            HivePageSourceProvider.ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;

            if (columnMapping.getKind() == PREFILLED) {
                String columnValue = columnMapping.getPrefilledValue();
                byte[] bytes = columnValue.getBytes(UTF_8);

                Object prefilledValue = getPartitionValue(hiveStorageTimeZone, name, type, columnValue, bytes);
                prefilledValueBuilder.put(columnIndex, new PrefilledValue(prefilledValue, type));
            }
        }
        ImmutableMap<Integer, PrefilledValue> prefilledValues = prefilledValueBuilder.build();
        if (prefilledValues.isEmpty()) {
            partitionValueAdapter = Optional.empty();
        }
        else {
            partitionValueAdapter = Optional.of(new PartitionValueAdapter(prefilledValues, size));
        }
    }

    private boolean isIdentity(Map<Integer, Integer> materializedTableToPartitionMap, int size)
    {
        // TODO, wrap in "Adaptation" and check against constant IDENTITY
        for (int i = 0; i < size; i++) {
            if (!materializedTableToPartitionMap.containsKey(i) || materializedTableToPartitionMap.get(i) != i) {
                return false;
            }
        }
        return true;
    }

    private Object getPartitionValue(DateTimeZone hiveStorageTimeZone, String name, Type type, String columnValue, byte[] bytes)
    {
        Object prefilledValue;
        if (isHiveNull(bytes)) {
            prefilledValue = null;
        }
        else if (type.equals(BOOLEAN)) {
            prefilledValue = booleanPartitionKey(columnValue, name);
        }
        else if (type.equals(BIGINT)) {
            prefilledValue = bigintPartitionKey(columnValue, name);
        }
        else if (type.equals(INTEGER)) {
            prefilledValue = integerPartitionKey(columnValue, name);
        }
        else if (type.equals(SMALLINT)) {
            prefilledValue = smallintPartitionKey(columnValue, name);
        }
        else if (type.equals(TINYINT)) {
            prefilledValue = tinyintPartitionKey(columnValue, name);
        }
        else if (type.equals(REAL)) {
            prefilledValue = floatPartitionKey(columnValue, name);
        }
        else if (type.equals(DOUBLE)) {
            prefilledValue = doublePartitionKey(columnValue, name);
        }
        else if (isVarcharType(type)) {
            prefilledValue = varcharPartitionKey(columnValue, name, type);
        }
        else if (isCharType(type)) {
            prefilledValue = charPartitionKey(columnValue, name, type);
        }
        else if (type.equals(DATE)) {
            prefilledValue = datePartitionKey(columnValue, name);
        }
        else if (type.equals(TIMESTAMP)) {
            prefilledValue = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
        }
        else if (isShortDecimal(type)) {
            prefilledValue = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
        }
        else if (isLongDecimal(type)) {
            prefilledValue = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
        }
        return prefilledValue;
    }

    Page adaptPage(Page dataPage)
    {
        // Reorder and add columns so they match the table's definition. This is first so that downstream adapters don't need to worry about partition vs table ordering.
        if (orderAdapter.isPresent()) {
            dataPage = orderAdapter.get().adaptPage(dataPage);
        }
        // Filter rows as necessary and drop unneeded columns
        if (bucketAdapter.isPresent()) {
            dataPage = bucketAdapter.get().adaptPage(dataPage);
        }
        // Fill in constant partition values
        if (partitionValueAdapter.isPresent()) {
            dataPage = partitionValueAdapter.get().adaptPage(dataPage);
        }
        // Coerce the types to match the table's definition
        if (typeAdapter.isPresent()) {
            dataPage = typeAdapter.get().adaptPage(dataPage);
        }
        return dataPage;
    }

    public class BucketAdapter
    {
        private final int[] bucketColumns;
        private final int bucketToKeep;
        private final int tableBucketCount;
        private final int partitionBucketCount; // for sanity check only
        private final List<TypeInfo> typeInfoList;
        // TODO need to strip off unused interim columns
        private final int[] bucketColumnsToKeep = new int[] {};

        public BucketAdapter(HivePageSourceProvider.BucketAdaptation bucketAdaptation)
        {
            this.bucketColumns = bucketAdaptation.getBucketColumnIndices();
            this.bucketToKeep = bucketAdaptation.getBucketToKeep();
            this.typeInfoList = bucketAdaptation.getBucketColumnHiveTypes().stream()
                    .map(HiveType::getTypeInfo)
                    .collect(toImmutableList());
            this.tableBucketCount = bucketAdaptation.getTableBucketCount();
            this.partitionBucketCount = bucketAdaptation.getPartitionBucketCount();
        }

        private Page extractColumns(Page page, int[] columns)
        {
            Block[] blocks = new Block[columns.length];
            for (int i = 0; i < columns.length; i++) {
                int dataColumn = columns[i];
                blocks[i] = page.getBlock(dataColumn);
            }
            return new Page(page.getPositionCount(), blocks);
        }

        public IntArrayList computeEligibleRowIds(Page page)
        {
            IntArrayList ids = new IntArrayList(page.getPositionCount());
            Page bucketColumnsPage = extractColumns(page, bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = getHiveBucket(tableBucketCount, typeInfoList, bucketColumnsPage, position);
                if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                    throw new PrestoException(HIVE_INVALID_BUCKET_FILES, format(
                            "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                            bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
                }
                if (bucket == bucketToKeep) {
                    ids.add(position);
                }
            }
            return ids;
        }

        Page adaptPage(Page dataPage)
        {
            IntArrayList rowsToKeep = computeEligibleRowIds(dataPage);
            Block[] adaptedBlocks = new Block[dataPage.getChannelCount()];
            for (int i = 0; i < adaptedBlocks.length; i++) {
                Block block = dataPage.getBlock(i);
                if (block instanceof LazyBlock && !((LazyBlock) block).isLoaded()) {
                    adaptedBlocks[i] = new LazyBlock(rowsToKeep.size(), new RowFilterLazyBlockLoader(dataPage.getBlock(i), rowsToKeep));
                }
                else {
                    adaptedBlocks[i] = block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size());
                }
            }
            // TODO: remove interim columns
            return new Page(rowsToKeep.size(), adaptedBlocks);
        }

        private final class RowFilterLazyBlockLoader
                implements LazyBlockLoader<LazyBlock>
        {
            private Block block;
            private final IntArrayList rowsToKeep;

            public RowFilterLazyBlockLoader(Block block, IntArrayList rowsToKeep)
            {
                this.block = requireNonNull(block, "block is null");
                this.rowsToKeep = requireNonNull(rowsToKeep, "rowsToKeep is null");
            }

            @Override
            public void load(LazyBlock lazyBlock)
            {
                if (block == null) {
                    return;
                }

                if (block instanceof LazyBlock) {
                    block = ((LazyBlock) block).getBlock();
                }
                lazyBlock.setBlock(block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size()));

                // clear reference to loader to free resources, since load was successful
                block = null;
            }
        }
    }

    private class OrderAdapter
    {
        // The source index for every output result. An empty entry indicates that the column isn't present in the source. These columns get filled with null
        private final Map<Integer, Integer> sourceIdxs;
        private final int size;

        public OrderAdapter(Map<Integer, Integer> orderAdaptation, int size)
        {
            this.size = size;
            // TODO move to its own method?
            sourceIdxs = orderAdaptation;
        }

        Page adaptPage(Page dataPage)
        {
            Block[] blocks = new Block[size];
            int batchSize = dataPage.getPositionCount();
            for (int i = 0; i < size; i++) {
                if (sourceIdxs.containsKey(i)) {
                    blocks[i] = dataPage.getBlock(sourceIdxs.get(i));
                }
                else {
                    blocks[i] = RunLengthEncodedBlock.create(types[i], null, batchSize);
                }
            }
            return new Page(batchSize, blocks);
        }
    }

    private class PartitionValueAdapter
    {
        private final Map<Integer, PrefilledValue> prefilledValues;
        private final int size;

        private PartitionValueAdapter(Map<Integer, PrefilledValue> prefilledValues, int size)
        {
            this.prefilledValues = prefilledValues;
            this.size = size;
        }

        Page adaptPage(Page dataPage)
        {
            Block[] blocks = new Block[size];
            int batchSize = dataPage.getPositionCount();
            for (int i = 0; i < size; i++) {
                if (prefilledValues.containsKey(i)) {
                    PrefilledValue constant = prefilledValues.get(i);
                    blocks[i] = RunLengthEncodedBlock.create(constant.type, constant.value, batchSize);
                }
                else {
                    blocks[i] = dataPage.getBlock(i);
                }
            }
            return new Page(batchSize, blocks);
        }
    }

    private class TypeAdapter
    {
        private final TypeAdaptation typeAdaptation;
        private final int size;

        public TypeAdapter(TypeAdaptation typeAdaptation, int size)
        {
            this.typeAdaptation = requireNonNull(typeAdaptation, "typeAdaptation is null");
            this.size = size;
        }

        Page adaptPage(Page dataPage)
        {
            Block[] blocks = new Block[size];
            int batchSize = dataPage.getPositionCount();
            for (int i = 0; i < size; i++) {
                if (typeAdaptation.requiresCoercion(i)) {
                    blocks[i] = new LazyBlock(batchSize, new CoercionLazyBlockLoader(dataPage.getBlock(i), typeAdaptation.getCoercer(i)));
                }
                else {
                    blocks[i] = dataPage.getBlock(i);
                }
            }
            return new Page(batchSize, blocks);
        }

        private final class CoercionLazyBlockLoader
                implements LazyBlockLoader<LazyBlock>
        {
            private final Function<Block, Block> coercer;
            private Block block;

            public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
            {
                this.block = requireNonNull(block, "block is null");
                this.coercer = requireNonNull(coercer, "coercer is null");
            }

            @Override
            public void load(LazyBlock lazyBlock)
            {
                if (block == null) {
                    return;
                }

                if (block instanceof LazyBlock) {
                    block = ((LazyBlock) block).getBlock();
                }
                lazyBlock.setBlock(coercer.apply(block));

                // clear reference to loader to free resources, since load was successful
                block = null;
            }
        }
    }

    private class TypeAdaptation
    {
        private final Map<Integer, Function<Block, Block>> coercers;

        public TypeAdaptation(Map<Integer, Function<Block, Block>> coercers)
        {
            this.coercers = requireNonNull(coercers);
        }

        public boolean requiresCoercion(int columnIndex)
        {
            return coercers.containsKey(columnIndex);
        }

        public Function<Block, Block> getCoercer(int columnIndex)
        {
            return coercers.get(columnIndex);
        }
    }

    private class PrefilledValue
    {
        private final Object value;
        private final Type type;

        private PrefilledValue(Object value, Type type)
        {
            this.value = value;
            this.type = type;
        }
    }
}
