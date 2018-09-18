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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.HiveSplit.BucketConversion;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.toPartitionColumnHandles;
import static com.facebook.presto.hive.HiveUtil.getPrefilledColumnValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final TypeManager typeManager;

    private final Set<HivePageSourceFactory> pageSourceFactories;

    @Inject
    public HivePageSourceProvider(
            HiveClientConfig hiveClientConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            TypeManager typeManager)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.hiveStorageTimeZone = hiveClientConfig.getDateTimeZone();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(requireNonNull(cursorProviders, "cursorProviders is null"));
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                hdfsEnvironment.getConfiguration(new HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path),
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileSize(),
                hiveSplit.getSchema(),
                hiveSplit.getEffectivePredicate(),
                hiveColumns,
                hiveSplit.getPartitionKeys(),
                hiveStorageTimeZone,
                typeManager,
                hiveSplit.getBucketConversion(),
                hiveSplit.getTableToPartitionMappings());
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
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> hiveColumns,
            List<HivePartitionKey> partitionKeys,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            Optional<BucketConversion> bucketConversion,
            TableToPartitionMappings tableToPartitionMappings)
    {
        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                partitionKeys,
                hiveColumns,
                bucketConversion.map(BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()),
                tableToPartitionMappings,
                bucketNumber, path);
        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(columnMappings);
        // Calculate the mapping from projected Table columns to projected columns
        // TODO: move this into the instantiation of the adapter
        ImmutableMap<Integer, Integer> materializedTableToPartitionMap = buildTableToPartitionMap(columnMappings, regularAndInterimColumnMappings);

        Optional<BucketAdaptation> bucketAdaptation = bucketConversion.map(conversion -> {
            // Table index
            Map<Integer, ColumnMapping> hiveIndexToPartitionIndex = uniqueIndex(regularAndInterimColumnMappings, columnMapping -> columnMapping.getHiveColumnHandle().getHiveColumnIndex());
            int[] bucketColumnIndices = conversion.getBucketColumnHandles().stream()
                    .mapToInt(columnHandle -> hiveIndexToPartitionIndex.get(columnHandle.getHiveColumnIndex()).getSourceIndex())
                    .toArray();
            List<HiveType> bucketColumnHiveTypes = conversion.getBucketColumnHandles().stream()
                    .map(columnHandle -> hiveIndexToPartitionIndex.get(columnHandle.getHiveColumnIndex()).getHiveColumnHandle().getHiveType())
                    .collect(toImmutableList());
            return new BucketAdaptation(bucketColumnIndices, bucketColumnHiveTypes, conversion.getTableBucketCount(), conversion.getPartitionBucketCount(), bucketNumber.getAsInt());
        });

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
                    toPartitionColumnHandles(regularAndInterimColumnMappings, tableToPartitionMappings, true),
                    effectivePredicate,
                    hiveStorageTimeZone);
            if (pageSource.isPresent()) {
                delegate = pageSource;
                break;
            }
        }

        if (!delegate.isPresent()) {
            for (HiveRecordCursorProvider provider : cursorProviders) {
                // GenericHiveRecordCursor will automatically do the coercion without the adaptor
                // TODO Pipe this into the adaptor? Is it necessary? Is it more performant
//                boolean doCoercion = !(provider instanceof GenericHiveRecordCursorProvider);
                boolean doCoercion = true;

                List<HiveColumnHandle> partitionColumns = toPartitionColumnHandles(regularAndInterimColumnMappings, tableToPartitionMappings, doCoercion);
                Optional<RecordCursor> cursor = provider.createRecordCursor(
                        configuration,
                        session,
                        path,
                        start,
                        length,
                        fileSize,
                        schema,
                        partitionColumns,
                        effectivePredicate,
                        hiveStorageTimeZone,
                        typeManager);

                if (cursor.isPresent()) {
                    RecordCursor delegateCursor = cursor.get();

                    List<Type> columnTypes = partitionColumns.stream()
                            .map(input -> typeManager.getType(input.getTypeSignature()))
                            .collect(toList());

                    delegate = Optional.of(new RecordPageSource(columnTypes, delegateCursor));
                    break;
                }
            }
        }
        if (delegate.isPresent()) {
            return Optional.of(
                    new HivePartitionAdapatingPageSource(
                            columnMappings,
                            materializedTableToPartitionMap,
                            bucketAdaptation,
                            hiveStorageTimeZone,
                            typeManager,
                            delegate.get()));
        }

        return Optional.empty();
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

    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;
        private final Optional<HiveType> coercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), coerceFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(), coerceFrom);
        }

        public static ColumnMapping interim(HiveColumnHandle hiveColumnHandle, int index)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.INTERIM, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), Optional.empty());
        }

        public static ColumnMapping nullMapping(HiveColumnHandle hiveColumnHandle, int index)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            // "\N" is Hive's magical byte sequence that represents nulls
            return new ColumnMapping(ColumnMappingKind.NULL, hiveColumnHandle, Optional.of("\\N"), OptionalInt.of(index), Optional.empty());
        }

        private ColumnMapping(ColumnMappingKind kind, HiveColumnHandle hiveColumnHandle, Optional<String> prefilledValue, OptionalInt index, Optional<HiveType> coerceFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.index = requireNonNull(index, "index is null");
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

        public int getSourceIndex()
        {
            checkState(kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM);
            return index.getAsInt();
        }

        public Optional<HiveType> getCoercionFrom()
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
                Path path)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
            int regularIndex = 0;
            Set<Integer> regularColumnIndices = new HashSet<>();
            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            for (HiveColumnHandle tableColumn : columns) {
                TableToPartitionMappings.TableToPartitionMap partitionMap = tableToPartitionMappings.getMapping(tableColumn.getHiveColumnIndex());
                Optional<HiveType> coercionFrom = Optional.ofNullable(partitionMap.getPartitionType());
                if (tableColumn.getColumnType() == REGULAR) {
                    checkArgument(regularColumnIndices.add(tableColumn.getHiveColumnIndex()), "duplicate hiveColumnIndex in columns list");
                    // If the column's in this partition, add it. Otherwise add null.
                    // TODO: Maybe move elsewhere?
                    if (partitionMap.isPresent()) {
                        columnMappings.add(regular(tableColumn, regularIndex, coercionFrom));
                    }
                    else {
                        columnMappings.add(nullMapping(tableColumn, regularIndex));
                    }
                    regularIndex++;
                }
                else {
                    columnMappings.add(prefilled(
                            tableColumn,
                            getPrefilledColumnValue(tableColumn, partitionKeysByName.get(tableColumn.getName()), path, bucketNumber),
                            coercionFrom));
                }
            }
            for (HiveColumnHandle column : requiredInterimColumns) {
                checkArgument(column.getColumnType() == REGULAR);
                if (regularColumnIndices.contains(column.getHiveColumnIndex())) {
                    continue; // This column exists in columns. Do not add it again.
                }
                // If coercion does not affect bucket number calculation, coercion doesn't need to be applied here.
                // Otherwise, read of this partition should not be allowed.
                // (Alternatively, the partition could be read as an unbucketed partition. This is not implemented.)
                columnMappings.add(interim(column, regularIndex));
                regularIndex++;
            }
            return columnMappings.build();
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM)
                    .collect(toImmutableList());
        }

        public static List<HiveColumnHandle> toPartitionColumnHandles(List<ColumnMapping> regularColumnMappings, TableToPartitionMappings tableToPartitionMappings, boolean doCoercion)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        HiveType columnType;
                        TypeSignature columnTypeSignature;

                        // TODO simplify
                        if (!doCoercion || !columnMapping.getCoercionFrom().isPresent()) {
                            columnType = columnHandle.getHiveType();
                            columnTypeSignature = columnHandle.getTypeSignature();
                        }
                        else {
                            columnType = columnMapping.getCoercionFrom().get();
                            columnTypeSignature = columnMapping.getCoercionFrom().get().getTypeSignature();
                        }

                        return new HiveColumnHandle(
                                columnHandle.getName(),
                                columnType,
                                columnTypeSignature,
                                tableToPartitionMappings.getMapping(columnHandle.getHiveColumnIndex()).getPartitionIndex(),
                                columnHandle.getColumnType(),
                                Optional.empty());
                    })
                    .collect(toList());
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
