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
import io.prestosql.spi.block.ColumnarMap;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;

import java.util.Optional;
import java.util.function.Function;

import static io.prestosql.spi.block.ColumnarMap.toColumnarMap;
import static java.util.Objects.requireNonNull;

class MapCoercer
        implements Function<Block, Block>
{
    private final Type toType;
    private final Function<Block, Block> keyCoercer;
    private final Function<Block, Block> valueCoercer;

    public MapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, boolean evolveByName)
    {
        requireNonNull(typeManager, "typeManage is null");
        requireNonNull(fromHiveType, "fromHiveType is null");
        this.toType = requireNonNull(toHiveType, "toHiveType is null").getType(typeManager);
        HiveType fromKeyHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
        HiveType fromValueHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
        HiveType toKeyHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
        HiveType toValueHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
        this.keyCoercer = fromKeyHiveType.equals(toKeyHiveType) ? null : Coercers.createCoercer(typeManager, fromKeyHiveType, toKeyHiveType, evolveByName);
        this.valueCoercer = fromValueHiveType.equals(toValueHiveType) ? null : Coercers.createCoercer(typeManager, fromValueHiveType, toValueHiveType, evolveByName);
    }

    @Override
    public Block apply(Block block)
    {
        ColumnarMap mapBlock = toColumnarMap(block);
        Block keysBlock = keyCoercer == null ? mapBlock.getKeysBlock() : keyCoercer.apply(mapBlock.getKeysBlock());
        Block valuesBlock = valueCoercer == null ? mapBlock.getValuesBlock() : valueCoercer.apply(mapBlock.getValuesBlock());
        boolean[] valueIsNull = new boolean[mapBlock.getPositionCount()];
        int[] offsets = new int[mapBlock.getPositionCount() + 1];
        for (int i = 0; i < mapBlock.getPositionCount(); i++) {
            valueIsNull[i] = mapBlock.isNull(i);
            offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
        }
        return ((MapType) toType).createBlockFromKeyValue(Optional.of(valueIsNull), offsets, keysBlock, valuesBlock);
    }
}
