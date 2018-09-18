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

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.util.HiveUtil;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarbinaryType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.prestosql.plugin.hive.coercer.Coercers.createCoercer;
import static io.prestosql.plugin.hive.util.HiveBucketing.getHiveBucket;
import static io.prestosql.plugin.hive.util.HiveUtil.bigintPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.booleanPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.charPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.datePartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.doublePartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.floatPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.integerPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.longDecimalPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.shortDecimalPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.smallintPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.timestampPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.tinyintPartitionKey;
import static io.prestosql.plugin.hive.util.HiveUtil.varcharPartitionKey;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

class PartitionAdapter
{
    private final Optional<BucketAdapter> bucketAdapter;
    private final Optional<OrderAdapter> interimOrderAdapter;
    private final Optional<OrderAdapter> outputOrderAdapter;
    private final Optional<TypeAdapter> typeAdapter;
    private final Optional<PartitionValueAdapter> partitionValueAdapter;
    // TODO: get rid of this, encapsulate in the order adapters
    private final Type[] types;

    PartitionAdapter(
            List<HivePageSourceProvider.ColumnMapping> columnMappings,
            Map<Integer, Integer> interimFromInputPermutation,
            int interimSize,
            Map<Integer, Integer> outputFromInterimPermutation,
            Optional<HivePageSourceProvider.BucketAdaptation> bucketAdapter,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            boolean evolveByName)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(interimFromInputPermutation, "interimFromInputPermutation is null");
        requireNonNull(outputFromInterimPermutation, "outputFromInterimPermutation is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");
        this.bucketAdapter = bucketAdapter.map(BucketAdapter::new);

        // Prepare order
        if (!isIdentity(interimFromInputPermutation, interimSize)) {
            interimOrderAdapter = Optional.of(new OrderAdapter(interimFromInputPermutation, interimSize));
        }
        else {
            interimOrderAdapter = Optional.empty();
        }

        int outputSize = outputFromInterimPermutation.size();
        if (interimSize != outputSize || !isIdentity(outputFromInterimPermutation, outputSize)) {
            outputOrderAdapter = Optional.of(new OrderAdapter(outputFromInterimPermutation, outputSize));
        }
        else {
            outputOrderAdapter = Optional.empty();
        }

        // Prepare types
        types = new Type[interimSize];
        for (int columnIndex = 0; columnIndex < interimSize; columnIndex++) {
            HivePageSourceProvider.ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;
        }

        // Prepare type adaptation
        ImmutableMap.Builder<Integer, Function<Block, Block>> coercersBuilder = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < interimSize; columnIndex++) {
            HivePageSourceProvider.ColumnMapping columnMapping = columnMappings.get(columnIndex);
            if (!columnMapping.getCoercionFrom().equals(columnMapping.getHiveColumnHandle().getHiveType())) {
                Function<Block, Block> coercer = createCoercer(typeManager, columnMapping.getCoercionFrom(), columnMapping.getHiveColumnHandle().getHiveType(), evolveByName);
                coercersBuilder.put(columnIndex, coercer);
            }
        }
        ImmutableMap<Integer, Function<Block, Block>> coercers = coercersBuilder.build();
        if (coercers.isEmpty()) {
            this.typeAdapter = Optional.empty();
        }
        else {
            TypeAdaptation typeAdaptation = new TypeAdaptation(coercers);
            this.typeAdapter = Optional.of(new TypeAdapter(typeAdaptation, interimSize));
        }

        // TODO Move to method
        // Prepare partition and prefilled value adaptation
        ImmutableMap.Builder<Integer, NullableValue> prefilledValueBuilder = ImmutableMap.builder();
        for (int columnIndex = 0; columnIndex < interimSize; columnIndex++) {
            HivePageSourceProvider.ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;

            if (columnMapping.getKind() == PREFILLED) {
                String columnValue = columnMapping.getPrefilledValue();

                Object value = getPartitionValue(hiveStorageTimeZone, name, type, columnValue);
                prefilledValueBuilder.put(columnIndex, new NullableValue(type, value));
            }
        }
        ImmutableMap<Integer, NullableValue> prefilledValues = prefilledValueBuilder.build();
        if (prefilledValues.isEmpty()) {
            partitionValueAdapter = Optional.empty();
        }
        else {
            partitionValueAdapter = Optional.of(new PartitionValueAdapter(prefilledValues, interimSize));
        }
    }

    private boolean isIdentity(Map<Integer, Integer> materializedTableToPartitionMap, int size)
    {
        // TODO, wrap in an "OrderAdaptation" class and check against an "Identity" subclass of that, rather than checking individually
        for (int i = 0; i < size; i++) {
            if (!materializedTableToPartitionMap.containsKey(i) || materializedTableToPartitionMap.get(i) != i) {
                return false;
            }
        }
        return true;
    }

    Page adaptPage(Page dataPage)
    {
        // Reorder and add columns so they match the table's definition. This is first so that downstream adapters don't need to worry about partition vs table ordering.
        if (interimOrderAdapter.isPresent()) {
            dataPage = interimOrderAdapter.get().adaptPage(dataPage);
        }
        // Fill in prefilled values, such as partitions, bucket, and hidden columns
        if (partitionValueAdapter.isPresent()) {
            dataPage = partitionValueAdapter.get().adaptPage(dataPage);
        }
        // Filter rows as necessary and drop unneeded columns
        if (bucketAdapter.isPresent()) {
            dataPage = bucketAdapter.get().adaptPage(dataPage);
        }
        // Coerce the types to match the table's definition
        if (typeAdapter.isPresent()) {
            dataPage = typeAdapter.get().adaptPage(dataPage);
        }
        // Remove any interim columns
        if (outputOrderAdapter.isPresent()) {
            dataPage = outputOrderAdapter.get().adaptPage(dataPage);
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

                lazyBlock.setBlock(block.getLoadedBlock().getPositions(rowsToKeep.elements(), 0, rowsToKeep.size()));

                // clear reference to loader to free resources, since load was successful
                block = null;
            }
        }
    }

    private class OrderAdapter
    {
        // The source index for every output result. An empty entry indicates that the column isn't present in the source. These columns get filled with null
        private final Map<Integer, Integer> sourceIdxs;
        private final int outputSize;

        public OrderAdapter(Map<Integer, Integer> orderAdaptation, int outputSize)
        {
            this.outputSize = outputSize;
            this.sourceIdxs = orderAdaptation;
        }

        Page adaptPage(Page dataPage)
        {
            Block[] blocks = new Block[outputSize];
            int batchSize = dataPage.getPositionCount();
            for (int i = 0; i < outputSize; i++) {
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
        private final Map<Integer, NullableValue> prefilledValues;
        private final int size;

        private PartitionValueAdapter(Map<Integer, NullableValue> prefilledValues, int size)
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
                    NullableValue constant = prefilledValues.get(i);
                    blocks[i] = RunLengthEncodedBlock.create(constant.getType(), constant.getValue(), batchSize);
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

                lazyBlock.setBlock(coercer.apply(block.getLoadedBlock()));

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

    private Object getPartitionValue(DateTimeZone hiveStorageTimeZone, String name, Type type, String columnValue)
    {
        Object prefilledValue;
        if (HiveUtil.isHiveNull(columnValue.getBytes(UTF_8))) {
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
        else if (type.equals(TIMESTAMP) || type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            prefilledValue = timestampPartitionKey(columnValue, hiveStorageTimeZone, name, type.equals(TIMESTAMP_WITH_TIME_ZONE));
        }
        else if (isShortDecimal(type)) {
            prefilledValue = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
        }
        else if (isLongDecimal(type)) {
            prefilledValue = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            prefilledValue = utf8Slice(columnValue);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
        }
        return prefilledValue;
    }
}
