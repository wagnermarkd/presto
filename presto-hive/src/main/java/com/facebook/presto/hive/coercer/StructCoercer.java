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
package com.facebook.presto.hive.coercer;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveUtil.extractStructFieldTypes;
import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

class StructCoercer
        implements Function<Block, Block>
{
    private final Function<Block, Block>[] coercers;
    private final Block[] nullBlocks;

    public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        requireNonNull(typeManager, "typeManage is null");
        requireNonNull(fromHiveType, "fromHiveType is null");
        requireNonNull(toHiveType, "toHiveType is null");
        List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
        List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
        this.coercers = new Function[toFieldTypes.size()];
        this.nullBlocks = new Block[toFieldTypes.size()];
        for (int i = 0; i < coercers.length; i++) {
            // TODO support coercion on names
            if (i >= fromFieldTypes.size()) {
                nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
            }
            else if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i))) {
                coercers[i] = Coercers.createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i));
            }
        }
    }

    @Override
    public Block apply(Block block)
    {
        ColumnarRow rowBlock = toColumnarRow(block);
        Block[] fields = new Block[coercers.length];
        int[] ids = new int[rowBlock.getField(0).getPositionCount()];
        for (int i = 0; i < coercers.length; i++) {
            if (coercers[i] != null) {
                fields[i] = coercers[i].apply(rowBlock.getField(i));
            }
            else if (i < rowBlock.getFieldCount()) {
                fields[i] = rowBlock.getField(i);
            }
            else {
                fields[i] = new DictionaryBlock(nullBlocks[i], ids);
            }
        }
        boolean[] valueIsNull = new boolean[rowBlock.getPositionCount()];
        for (int i = 0; i < rowBlock.getPositionCount(); i++) {
            valueIsNull[i] = rowBlock.isNull(i);
        }
        return RowBlock.fromFieldBlocks(valueIsNull, fields);
    }
}
