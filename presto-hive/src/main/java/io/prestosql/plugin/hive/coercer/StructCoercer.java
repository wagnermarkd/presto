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
package io.prestosql.plugin.hive.coercer;

import io.prestosql.plugin.hive.HiveType;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.plugin.hive.util.HiveUtil.extractStructFieldTypes;
import static io.prestosql.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

class StructCoercer
        implements Function<Block, Block>
{
    private final Function<Block, Block>[] coercers;
    private final Block[] nullBlocks;
    private final int[] sourcePosition;

    public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, boolean evolveByName)
    {
        requireNonNull(typeManager, "typeManage is null");
        requireNonNull(fromHiveType, "fromHiveType is null");
        requireNonNull(toHiveType, "toHiveType is null");
        List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
        List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
        this.coercers = new Function[toFieldTypes.size()];
        this.sourcePosition = new int[toFieldTypes.size()];
        this.nullBlocks = new Block[toFieldTypes.size()];
        if (evolveByName) {
            prepareCoerceByName(fromHiveType, toHiveType, typeManager, evolveByName);
        }
        else {
            prepareCoerceByPosition(typeManager, evolveByName, fromFieldTypes, toFieldTypes);
        }
    }

    private void prepareCoerceByName(HiveType fromHiveType, HiveType toHiveType, TypeManager typeManager, boolean evolveByName)
    {
        // Lowercase everything to do case insensitive comparison
        List<String> fromFieldNames = ((StructTypeInfo) fromHiveType.getTypeInfo()).getAllStructFieldNames().stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> toFieldNames = ((StructTypeInfo) toHiveType.getTypeInfo()).getAllStructFieldNames().stream().map(String::toLowerCase).collect(Collectors.toList());
        List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
        List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);

        for (int i = 0; i < toFieldNames.size(); i++) {
            int sourceIndex = fromFieldNames.indexOf(toFieldNames.get(i));
            HiveType toType = toFieldTypes.get(i);
            // Fields that aren't in the source will be null
            if (sourceIndex == -1) {
                nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                sourcePosition[i] = -1;
            }
            else {
                sourcePosition[i] = sourceIndex;
                HiveType fromType = fromFieldTypes.get(sourceIndex);
                if (!fromType.equals(toType)) {
                    coercers[i] = Coercers.createCoercer(typeManager, fromType, toFieldTypes.get(i), evolveByName);
                }
            }
        }
    }

    private void prepareCoerceByPosition(TypeManager typeManager, boolean evolveByName, List<HiveType> fromFieldTypes, List<HiveType> toFieldTypes)
    {
        for (int i = 0; i < coercers.length; i++) {
            if (i >= fromFieldTypes.size()) {
                nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                sourcePosition[i] = -1;
            }
            else if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i))) {
                coercers[i] = Coercers.createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i), evolveByName);
                sourcePosition[i] = i;
            }
        }
    }

    @Override
    public Block apply(Block block)
    {
        ColumnarRow rowBlock = toColumnarRow(block);
        Block[] fields = new Block[sourcePosition.length];
        int nonNullCount = rowBlock.getField(0).getPositionCount();
        for (int i = 0; i < sourcePosition.length; i++) {
            int source = sourcePosition[i];
            if (source == -1) {
                fields[i] = new RunLengthEncodedBlock(nullBlocks[i], nonNullCount);
            }
            else {
                if (coercers[i] == null) {
                    fields[i] = rowBlock.getField(source);
                }
                else {
                    fields[i] = coercers[i].apply(rowBlock.getField(source));
                }
            }
        }
        boolean[] valueIsNull = new boolean[rowBlock.getPositionCount()];
        for (int i = 0; i < rowBlock.getPositionCount(); i++) {
            valueIsNull[i] = rowBlock.isNull(i);
        }
        return RowBlock.fromFieldBlocks(valueIsNull.length, Optional.of(valueIsNull), fields);
    }
}
