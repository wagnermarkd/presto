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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveSplit.BucketConversion;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.prestosql.plugin.hive.util.HiveUtil.getPrefilledColumnValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final TypeManager typeManager;

    private final Set<HivePageSourceFactory> pageSourceFactories;
    private final boolean evolveByName;

    @Inject
    public HivePageSourceProvider(
            HiveConfig hiveConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            TypeManager typeManager)
    {
        requireNonNull(hiveConfig, "hiveConfig is null");
        this.hiveStorageTimeZone = hiveConfig.getDateTimeZone();
        this.evolveByName = hiveConfig.isEvolveByName();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(requireNonNull(cursorProviders, "cursorProviders is null"));
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path);

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                configuration,
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileSize(),
                hiveSplit.getFileModifiedTime(),
                hiveSplit.getSchema(),
                hiveTable.getCompactEffectivePredicate(),
                hiveColumns,
                hiveSplit.getPartitionKeys(),
                hiveStorageTimeZone,
                typeManager,
                hiveSplit.getBucketConversion(),
                hiveSplit.isS3SelectPushdownEnabled(),
                hiveSplit.getTableToPartitionMappings(),
                evolveByName);
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new RuntimeException("Could not find a file reader for split " + hiveSplit);
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt bucketNumber,
            long start,
            long length,
            long fileSize,
            long fileModifiedTime,
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> hiveColumns,
            List<HivePartitionKey> partitionKeys,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            Optional<BucketConversion> bucketConversion,
            boolean s3SelectPushdownEnabled,
            TableToPartitionMappings tableToPartitionMappings,
            boolean evolveByName)
    {
        // Fill constant values. These will be formed in their own page

        // Reorder columns
        // For REGULAR columns, they will be retrieved from the appropriate inputIndex in the partition's schema
        // FOR SYNTHESIZED or PARTITION_KEY, they will be retrieved from the prefilled page

        // Coerce types
        // All columns will be mapped to their appropriate type

        // Filter according to bucket mapping
        // Rows will be filtered out that don't match bucketing

        // Remove interim-only columns
        // Only columns that are finally needed are retained
        // Should have an "interim" position, but for non-interim columns, the "interim" position should also be the final position. This has the effect that all interim columns
        // will be at the end of the Page.

        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                partitionKeys,
                hiveColumns,
                bucketConversion.map(BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()),
                tableToPartitionMappings,
                bucketNumber, path,
                fileSize,
                fileModifiedTime);

        // These are the Column
        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);
        // Calculate the mapping from projected Table columns to projected columns
        // TODO: move this into the instantiation of the adapter

        Map<Integer, Integer> interimFromInputPermutation = ColumnMapping.getInterimFromInputPermutation(columnMappings);
        Map<Integer, Integer> outputFromInterimPermutation = ColumnMapping.getOutputFromInterimPermutation(columnMappings);

        Optional<BucketAdaptation> bucketAdaptation = bucketConversion.map(conversion -> getBucketAdaptation(bucketNumber, regularAndInterimColumnMappings, conversion));

        Optional<? extends ConnectorPageSource> delegate = Optional.empty();
        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    ColumnMapping.toReaderColumnHandles(regularAndInterimColumnMappings, tableToPartitionMappings),
                    // TODO: (mwagner) map this to reader column as well
                    effectivePredicate,
                    hiveStorageTimeZone);
            if (pageSource.isPresent()) {
                delegate = pageSource;
                break;
            }
        }

        if (!delegate.isPresent()) {
            for (HiveRecordCursorProvider provider : cursorProviders) {
                List<HiveColumnHandle> partitionColumns =
                        ColumnMapping.toReaderColumnHandles(regularAndInterimColumnMappings, tableToPartitionMappings);
                Optional<RecordCursor> cursor = provider.createRecordCursor(
                        configuration,
                        session,
                        path,
                        start,
                        length,
                        fileSize,
                        schema,
                        partitionColumns,
                        // TODO: (mwagner) map this to reader column as well
                        effectivePredicate,
                        hiveStorageTimeZone,
                        typeManager,
                        s3SelectPushdownEnabled);

                if (cursor.isPresent()) {
                    RecordCursor delegateCursor = cursor.get();

                    List<Type> columnTypes = partitionColumns.stream()
                            .map(input -> typeManager.getType(input.getTypeSignature()))
                            .collect(toList());

                    // TODO: (mwagner) We need a parallel cursor implementation of the adaptor pattern
                    delegate = Optional.of(new RecordPageSource(columnTypes, delegateCursor));
                    break;
                }
            }
        }
        if (delegate.isPresent()) {
            return Optional.of(
                    new HivePartitionAdapatingPageSource(
                            columnMappings,
                            interimFromInputPermutation,
                            // TODO: Consider getting this from column mappings. See notes in PartitionAdapter as well
                            columnMappings.size(),
                            outputFromInterimPermutation,
                            bucketAdaptation,
                            hiveStorageTimeZone,
                            typeManager,
                            delegate.get(),
                            evolveByName));
        }

        return Optional.empty();
    }

    private static BucketAdaptation getBucketAdaptation(OptionalInt bucketNumber, List<ColumnMapping> regularAndInterimColumnMappings, BucketConversion conversion)
    {
        // Table inputIndex
        Map<Integer, ColumnMapping> hiveIndexToPartitionIndex = uniqueIndex(regularAndInterimColumnMappings, columnMapping -> columnMapping.getHiveColumnHandle().getHiveColumnIndex());
        // The BucketAdapter works on interim ordering of columns. Any columns that aren't needed in the final output will be discarded later.
        int[] bucketColumnIndices = conversion.getBucketColumnHandles().stream()
                .mapToInt(columnHandle -> hiveIndexToPartitionIndex.get(columnHandle.getHiveColumnIndex()).getInterimIndex())
                .toArray();
        List<HiveType> bucketColumnHiveTypes = conversion.getBucketColumnHandles().stream()
                .map(columnHandle -> hiveIndexToPartitionIndex.get(columnHandle.getHiveColumnIndex()).getHiveColumnHandle().getHiveType())
                .collect(toImmutableList());
        return new BucketAdaptation(bucketColumnIndices, bucketColumnHiveTypes, conversion.getTableBucketCount(), conversion.getPartitionBucketCount(), bucketNumber.getAsInt());
    }

    private static ImmutableMap<Integer, Integer> buildTableToPartitionMap(List<ColumnMapping> columnMappings, List<ColumnMapping> regularAndInterimColumnMappings)
    {
        ImmutableMap.Builder<Integer, Integer> materializedTableToPartition = ImmutableMap.builder();
        int partitionIdx = 0;
        for (int tableIdx = 0; tableIdx < columnMappings.size() && partitionIdx < regularAndInterimColumnMappings.size(); tableIdx++) {
            if (regularAndInterimColumnMappings.get(partitionIdx).equals(columnMappings.get(tableIdx))) {
                materializedTableToPartition.put(tableIdx, partitionIdx);
                partitionIdx++;
            }
        }
        return materializedTableToPartition.build();
    }

    // TODO: Find a better name: this class is essentially the plan for how each column will be transformed
    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        // Ordinal of this column in the underlying page source or record cursor
        private final OptionalInt inputIndex;
        // Ordinal of this column in the intermediate Page. Columns may exist in the intermediate pages only. For example, when computing bucket adaptations.
        // For columns returned to the engine, we expect this to be the same as outputIndex. It's included here for clarity.
        private final int interimIndex;
        // Ordinal of this column in the final Page, which will be returned to the engine
        private final OptionalInt outputIndex;
        private final HiveType coercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int inputIndex, int interimIndex, int outputIndex, HiveType coercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(inputIndex),
                    interimIndex,
                    OptionalInt.of(outputIndex),
                    coercionFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue, int interimIndex, int outputIndex)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(),
                    interimIndex,
                    OptionalInt.of(outputIndex),
                    hiveColumnHandle.getHiveType());
        }

        public static ColumnMapping interim(HiveColumnHandle hiveColumnHandle, int inputIndex, int interimIndex, HiveType coercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.INTERIM, hiveColumnHandle, Optional.empty(), OptionalInt.of(inputIndex),
                    interimIndex,
                    OptionalInt.empty(),
                    coercionFrom);
        }

        public static ColumnMapping nullMapping(HiveColumnHandle hiveColumnHandle, int interimIndex, int outputIndex, HiveType coercionFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            // "\N" is Hive's magical byte sequence that represents nulls
            return new ColumnMapping(ColumnMappingKind.NULL, hiveColumnHandle, Optional.of("\\N"), OptionalInt.of(hiveColumnHandle.getHiveColumnIndex()),
                    interimIndex,
                    OptionalInt.of(outputIndex),
                    coercionFrom);
        }

        private ColumnMapping(
                ColumnMappingKind kind,
                HiveColumnHandle hiveColumnHandle,
                Optional<String> prefilledValue,
                OptionalInt inputIndex,
                int interimIndex,
                OptionalInt outputIndex,
                HiveType coerceFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.inputIndex = requireNonNull(inputIndex, "inputIndex is null");
            this.interimIndex = requireNonNull(interimIndex, "interimIndex is null");
            this.outputIndex = requireNonNull(outputIndex, "outputIndex is null");
            this.coercionFrom = requireNonNull(coerceFrom, "coerceFrom is null");
        }

        public ColumnMappingKind getKind()
        {
            return kind;
        }

        public String getPrefilledValue()
        {
            checkState(kind == ColumnMappingKind.PREFILLED);
            return prefilledValue.get();
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getInputIndex()
        {
            checkState(kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM);
            return inputIndex.getAsInt();
        }

        public int getInterimIndex()
        {
            return interimIndex;
        }

        public int getOutputIndex()
        {
            checkState(kind != ColumnMappingKind.INTERIM);
            return outputIndex.getAsInt();
        }

        public HiveType getCoercionFrom()
        {
            return coercionFrom;
        }

        /**
         * @param columns columns that need to be returned to engine
         * @param requiredInterimColumns columns that are needed for processing, but shouldn't be returned to engine (may overlaps with columns)
         * @param tableToPartitionMappings mapping class from the table's schema to the partition's
         * @param bucketNumber empty if table is not bucketed, a number within [0, # bucket in table) otherwise
         */
        public static List<ColumnMapping> buildColumnMappings(
                List<HivePartitionKey> partitionKeys,
                List<HiveColumnHandle> columns,
                List<HiveColumnHandle> requiredInterimColumns,
                TableToPartitionMappings tableToPartitionMappings,
                OptionalInt bucketNumber,
                Path path,
                long fileSize,
                long fileModifiedTime)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
            int inputIndex = 0;
            int interimIndex = 0;
            int outputIndex = 0;
            Set<Integer> inputColumnIndices = new HashSet<>();
            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();

            // For each column that has been asked for by the engine, create a ColumnMapping for it. The order of the columns in the output page will be in the same order as the
            // engine has asked for. For ease of debugging, we also maintain that order during interim processing.
            for (HiveColumnHandle tableColumn : columns) {
                TableToPartitionMappings.TableToPartitionMap partitionMap = tableToPartitionMappings.getMapping(tableColumn.getHiveColumnIndex());
                if (tableColumn.getColumnType() == REGULAR) {
                    checkArgument(inputColumnIndices.add(tableColumn.getHiveColumnIndex()), "duplicate hiveColumnIndex in columns list");
                    // If the column's in this partition, add it. Otherwise add null.
                    if (partitionMap.isPresent()) {
                        columnMappings.add(regular(tableColumn, inputIndex, interimIndex, outputIndex, partitionMap.getPartitionType()));
                        inputIndex++;
                    }
                    else {
                        columnMappings.add(nullMapping(tableColumn, interimIndex, outputIndex, tableColumn.getHiveType()));
                    }
                }
                else {
                    columnMappings.add(prefilled(
                            tableColumn,
                            getPrefilledColumnValue(tableColumn, partitionKeysByName.get(tableColumn.getName()), path, bucketNumber, fileSize, fileModifiedTime),
                            interimIndex, outputIndex));
                }
                interimIndex++;
                outputIndex++;
            }
            // Some columns may be needed for intermediate processing. Some of these may not been asked for by the engine. Any additional columns needed will go at the
            // end of the interim page
            for (HiveColumnHandle column : requiredInterimColumns) {
                checkArgument(column.getColumnType() == REGULAR);
                if (inputColumnIndices.contains(column.getHiveColumnIndex())) {
                    continue; // This column exists in columns. Do not add it again.
                }
                // TODO: (mwagner) bucket mapping should work with coercion
                // If coercion does not affect bucket number calculation, coercion doesn't need to be applied here.
                // Otherwise, read of this partition should not be allowed.
                // (Alternatively, the partition could be read as an unbucketed partition. This is not implemented.)
                TableToPartitionMappings.TableToPartitionMap partitionMap = tableToPartitionMappings.getMapping(column.getHiveColumnIndex());
                columnMappings.add(interim(column, inputIndex, interimIndex, partitionMap.getPartitionType()));
                interimIndex++;
                inputIndex++;
            }
            return columnMappings.build();
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM)
                    .collect(toImmutableList());
        }

        // HivePageSourceFactories and HiveRecordCursorProviders need HiveColumnHandles to know what to read. We need to map the table handles into equivalent partition level ones
        public static List<HiveColumnHandle> toReaderColumnHandles(List<ColumnMapping> regularColumnMappings, TableToPartitionMappings tableToPartitionMappings)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();

                        return new HiveColumnHandle(
                                columnHandle.getName(),
                                columnMapping.getCoercionFrom(),
                                columnMapping.getCoercionFrom().getTypeSignature(),
                                tableToPartitionMappings.getMapping(columnHandle.getHiveColumnIndex()).getPartitionIndex(),
                                columnHandle.getColumnType(),
                                Optional.empty());
                    })
                    .collect(toList());
        }

        public static Map<Integer, Integer> getInterimFromInputPermutation(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(x -> x.getKind() != ColumnMappingKind.PREFILLED && x.getKind() != ColumnMappingKind.NULL)
                    .collect(toMap(
                            x -> x.getInterimIndex(),
                            x -> x.getInputIndex()));
        }

        public static Map<Integer, Integer> getOutputFromInterimPermutation(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(x -> x.getKind() != ColumnMappingKind.INTERIM)
                    .collect(toMap(
                            x -> x.getOutputIndex(),
                            x -> x.getInterimIndex()));
        }
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
        INTERIM,
        NULL
    }

    public static class BucketAdaptation
    {
        private final int[] bucketColumnIndices;
        private final List<HiveType> bucketColumnHiveTypes;
        private final int tableBucketCount;
        private final int partitionBucketCount;
        private final int bucketToKeep;

        public BucketAdaptation(int[] bucketColumnIndices, List<HiveType> bucketColumnHiveTypes, int tableBucketCount, int partitionBucketCount, int bucketToKeep)
        {
            this.bucketColumnIndices = bucketColumnIndices;
            this.bucketColumnHiveTypes = bucketColumnHiveTypes;
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.bucketToKeep = bucketToKeep;
        }

        public int[] getBucketColumnIndices()
        {
            return bucketColumnIndices;
        }

        public List<HiveType> getBucketColumnHiveTypes()
        {
            return bucketColumnHiveTypes;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getPartitionBucketCount()
        {
            return partitionBucketCount;
        }

        public int getBucketToKeep()
        {
            return bucketToKeep;
        }
    }
}
