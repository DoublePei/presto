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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;

import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.Reflection.methodHandle;

/**
 * @author jinhai
 * @date 2020/10/22
 */
public class StringToArrayCast
        extends SqlOperator
{
    public static final StringToArrayCast STRING_TO_ARRAY = new StringToArrayCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(StringToArrayCast.class, "toArray", Type.class, ConnectorSession.class, Slice.class);

    private StringToArrayCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature("array(varchar)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR)));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(VARCHAR);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block toArray(Type type, ConnectorSession session, Slice slice)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        type.writeSlice(blockBuilder, slice);
        return blockBuilder.build();
    }
}
