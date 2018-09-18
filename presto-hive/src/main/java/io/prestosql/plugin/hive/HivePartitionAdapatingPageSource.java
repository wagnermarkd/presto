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

import io.prestosql.plugin.hive.HivePageSourceProvider.BucketAdaptation;
import io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.util.Objects.requireNonNull;

public class HivePartitionAdapatingPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final PartitionAdapter adapter;

    public HivePartitionAdapatingPageSource(
            List<ColumnMapping> columnMappings,
            Map<Integer, Integer> interimFromInputPermutation,
            // TODO: Encapsulate
            int interimSize,
            Map<Integer, Integer> outputFromInterimPermutation,
            Optional<BucketAdaptation> bucketAdaptation,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorPageSource delegate,
            boolean evolveByName)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");

        this.adapter = new PartitionAdapter(columnMappings,
                interimFromInputPermutation,
                interimSize,
                outputFromInterimPermutation,
                bucketAdaptation,
                hiveStorageTimeZone,
                typeManager,
                evolveByName);
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            return adapter.adaptPage(dataPage);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }
}
