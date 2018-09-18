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

import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.plugin.hive.HiveType.HIVE_BYTE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.prestosql.plugin.hive.HiveType.HIVE_FLOAT;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_SHORT;
import static io.prestosql.plugin.hive.util.HiveUtil.extractStructFieldTypes;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class HiveCoercionPolicy
        implements CoercionPolicy
{
    private final TypeManager typeManager;
    private final HiveConfig config;

    @Inject
    public HiveCoercionPolicy(HiveConfig hiveConfig, TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(hiveConfig, "hiveConfig is null");
    }

    @Override
    public boolean canCoerce(HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (fromType instanceof VarcharType) {
            return toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG);
        }
        if (toType instanceof VarcharType) {
            return fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_BYTE)) {
            return toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_SHORT)) {
            return toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_INT)) {
            return toHiveType.equals(HIVE_LONG);
        }
        if (fromHiveType.equals(HIVE_FLOAT)) {
            return toHiveType.equals(HIVE_DOUBLE) || toType instanceof DecimalType;
        }
        if (fromHiveType.equals(HIVE_DOUBLE)) {
            return toHiveType.equals(HIVE_FLOAT) || toType instanceof DecimalType;
        }
        if (fromType instanceof DecimalType) {
            return toType instanceof DecimalType || toHiveType.equals(HIVE_FLOAT) || toHiveType.equals(HIVE_DOUBLE);
        }

        return canCoerceForList(fromHiveType, toHiveType) || canCoerceForMap(fromHiveType, toHiveType) || canCoerceForStruct(fromHiveType, toHiveType);
    }

    private boolean canCoerceForMap(HiveType fromHiveType, HiveType toHiveType)
    {
        if (!fromHiveType.getCategory().equals(Category.MAP) || !toHiveType.getCategory().equals(Category.MAP)) {
            return false;
        }
        HiveType fromKeyType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
        HiveType fromValueType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
        HiveType toKeyType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
        HiveType toValueType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
        return (fromKeyType.equals(toKeyType) || canCoerce(fromKeyType, toKeyType)) &&
                (fromValueType.equals(toValueType) || canCoerce(fromValueType, toValueType));
    }

    private boolean canCoerceForList(HiveType fromHiveType, HiveType toHiveType)
    {
        if (!fromHiveType.getCategory().equals(Category.LIST) || !toHiveType.getCategory().equals(Category.LIST)) {
            return false;
        }
        HiveType fromElementType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
        HiveType toElementType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
        return fromElementType.equals(toElementType) || canCoerce(fromElementType, toElementType);
    }

    private boolean canCoerceForStruct(HiveType fromHiveType, HiveType toHiveType)
    {
        if (!fromHiveType.getCategory().equals(Category.STRUCT) || !toHiveType.getCategory().equals(Category.STRUCT)) {
            return false;
        }

        if (config.isEvolveByName()) {
            return canCoerceForStructByName(fromHiveType, toHiveType);
        }
        else {
            return canCoerceForStructByPosition(fromHiveType, toHiveType);
        }
    }

    private boolean canCoerceForStructByName(HiveType fromHiveType, HiveType toHiveType)
    {
        // Lowercase everything to do case insensitive comparison
        List<String> fromFieldNames = ((StructTypeInfo) fromHiveType.getTypeInfo()).getAllStructFieldNames().stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> toFieldNames = ((StructTypeInfo) toHiveType.getTypeInfo()).getAllStructFieldNames().stream().map(String::toLowerCase).collect(Collectors.toList());
        List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
        List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);

        for (int i = 0; i < toFieldNames.size(); i++) {
            int sourceIndex = fromFieldNames.indexOf(toFieldNames.get(i));
            // Fields that aren't in the source will be null
            if (sourceIndex == -1) {
                continue;
            }
            HiveType fromType = fromFieldTypes.get(sourceIndex);
            HiveType toType = toFieldTypes.get(i);
            if (fromType.equals(toType) || canCoerce(fromType, toType)) {
                continue;
            }
            return false;
        }

        return true;
    }

    private boolean canCoerceForStructByPosition(HiveType fromHiveType, HiveType toHiveType)
    {
        List<String> fromFieldNames = ((StructTypeInfo) fromHiveType.getTypeInfo()).getAllStructFieldNames();
        List<String> toFieldNames = ((StructTypeInfo) toHiveType.getTypeInfo()).getAllStructFieldNames();
        List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
        List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
        // Rule:
        // * Fields may be added or dropped from the end.
        // * For all other field indices, the corresponding fields must have
        //   the same name, and the type must be coercible.
        for (int i = 0; i < min(fromFieldTypes.size(), toFieldTypes.size()); i++) {
            if (!fromFieldNames.get(i).equals(toFieldNames.get(i))) {
                return false;
            }
            if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i)) && !canCoerce(fromFieldTypes.get(i), toFieldTypes.get(i))) {
                return false;
            }
        }
        return true;
    }
}
