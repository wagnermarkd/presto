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
import io.prestosql.plugin.hive.coercions.DoubleToFloatCoercer;
import io.prestosql.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.prestosql.plugin.hive.coercions.IntegerNumberUpscaleCoercer;
import io.prestosql.plugin.hive.coercions.VarcharToIntegerNumberCoercer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;

import java.util.function.Function;

import static io.prestosql.plugin.hive.HiveType.HIVE_BYTE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.prestosql.plugin.hive.HiveType.HIVE_FLOAT;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_SHORT;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.prestosql.plugin.hive.util.HiveUtil.isArrayType;
import static io.prestosql.plugin.hive.util.HiveUtil.isMapType;
import static io.prestosql.plugin.hive.util.HiveUtil.isRowType;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.String.format;

public class Coercers
{
    private Coercers() {}

    public static Function<Block, Block> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, boolean evolveByName)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return new IntegerNumberToVarcharCoercer<>(fromType, (VarcharType) toType);
        }
        if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return new VarcharToIntegerNumberCoercer<>((VarcharType) fromType, toType);
        }
        if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer<>(fromType, toType);
        }
        if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer<>(fromType, toType);
        }
        if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer<>(fromType, toType);
        }
        if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return new FloatToDoubleCoercer();
        }
        if (fromHiveType.equals(HIVE_DOUBLE) && toHiveType.equals(HIVE_FLOAT)) {
            return new DoubleToFloatCoercer();
        }
        if (fromType instanceof DecimalType && toType instanceof DecimalType) {
            return createDecimalToDecimalCoercer((DecimalType) fromType, (DecimalType) toType);
        }
        if (fromType instanceof DecimalType && toType == DOUBLE) {
            return createDecimalToDoubleCoercer((DecimalType) fromType);
        }
        if (fromType instanceof DecimalType && toType == REAL) {
            return createDecimalToRealCoercer((DecimalType) fromType);
        }
        if (fromType == DOUBLE && toType instanceof DecimalType) {
            return createDoubleToDecimalCoercer((DecimalType) toType);
        }
        if (fromType == REAL && toType instanceof DecimalType) {
            return createRealToDecimalCoercer((DecimalType) toType);
        }
        if (isArrayType(fromType) && isArrayType(toType)) {
            return new ListCoercer(typeManager, fromHiveType, toHiveType, evolveByName);
        }
        if (isMapType(fromType) && isMapType(toType)) {
            return new MapCoercer(typeManager, fromHiveType, toHiveType, evolveByName);
        }
        if (isRowType(fromType) && isRowType(toType)) {
            return new StructCoercer(typeManager, fromHiveType, toHiveType, evolveByName);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }
}
