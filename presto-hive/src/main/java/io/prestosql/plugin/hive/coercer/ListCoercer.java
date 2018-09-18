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
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ColumnarArray;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;

import java.util.Optional;
import java.util.function.Function;

import static io.prestosql.spi.block.ColumnarArray.toColumnarArray;
import static java.util.Objects.requireNonNull;

public class ListCoercer
        implements Function<Block, Block>
{
    private final Function<Block, Block> elementCoercer;

    public ListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, boolean evolveByName)
    {
        requireNonNull(typeManager, "typeManage is null");
        requireNonNull(fromHiveType, "fromHiveType is null");
        requireNonNull(toHiveType, "toHiveType is null");
        HiveType fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
        HiveType toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
        this.elementCoercer = fromElementHiveType.equals(toElementHiveType) ? null : Coercers.createCoercer(typeManager, fromElementHiveType, toElementHiveType, evolveByName);
    }

    @Override
    public Block apply(Block block)
    {
        if (elementCoercer == null) {
            return block;
        }
        ColumnarArray arrayBlock = toColumnarArray(block);
        Block elementsBlock = elementCoercer.apply(arrayBlock.getElementsBlock());
        boolean[] valueIsNull = new boolean[arrayBlock.getPositionCount()];
        int[] offsets = new int[arrayBlock.getPositionCount() + 1];
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            valueIsNull[i] = arrayBlock.isNull(i);
            offsets[i + 1] = offsets[i] + arrayBlock.getLength(i);
        }
        return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), Optional.of(valueIsNull), offsets, elementsBlock);
    }
}
