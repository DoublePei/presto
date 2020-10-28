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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.type.UnknownType;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Reflection.methodHandle;
import static java.util.Collections.nCopies;

public final class ConcatWsFunction
        extends SqlScalarFunction
{
    public static final ImmutableSet<String> supportedTypes = ImmutableSet.of(StandardTypes.VARCHAR, "array(varchar)", UnknownType.NAME);
    public static final ConcatWsFunction CONCAT_WS_FUNCTION = new ConcatWsFunction();

    private ConcatWsFunction()
    {
        super(new Signature(
                "concat_ws",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.VARCHAR),
                ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature("array(varchar)")),
                true));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "concatenates given strings";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string concatenation");

        Class<?> clazz = generateConcatWs(arity);

        ImmutableList.Builder<Class<?>> parameterClasses = ImmutableList.builder();
        parameterClasses.add(Slice.class).addAll(nCopies(arity - 1, Block.class));
        MethodHandle methodHandle = methodHandle(clazz, "concatWs", parameterClasses.build().toArray(new Class<?>[arity]));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateConcatWs(int arity)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("varchar_concatWs" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concatWs()
        List<Parameter> parameters = new ArrayList<>(arity);
        parameters.add(arg("arg0", Slice.class));
        IntStream.range(1, arity).forEach(i -> parameters.add(arg("arg" + i, Block.class)));

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "concatWs", type(Slice.class), parameters);
        BytecodeBlock body = method.getBody();
        body.append(invokeStatic(ConcatWsFunction.class, "concatWsCode", Slice.class, parameters.get(0),
                newArray(ParameterizedType.type(Block[].class), parameters.subList(1, parameters.size())))).retObject();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(ConcatWsFunction.class.getClassLoader()));
    }

    @UsedByGeneratedCode
    public static Slice concatWsCode(Slice slice, Block[] blocks)
    {
        List<String> parts = new ArrayList<>();
        for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
            Block block = blocks[blockIndex];
            for (int i = 0; i < block.getPositionCount(); i++) {
                parts.add(VARCHAR.getSlice(block, i).toStringUtf8());
            }
        }
        return Slices.utf8Slice(Joiner.on(slice.toStringUtf8()).join(parts));
    }
}
