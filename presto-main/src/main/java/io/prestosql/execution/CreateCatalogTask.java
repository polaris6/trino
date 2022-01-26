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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.metadata.DynamicCatalogManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.tree.CatalogElement;
import io.prestosql.sql.tree.CreateCatalog;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class CreateCatalogTask
        implements DataDefinitionTask<CreateCatalog>
{
    private DynamicCatalogManager dynamicCatalogManager;

    @Inject
    public CreateCatalogTask(DynamicCatalogManager dynamicCatalogManager)
    {
        requireNonNull(dynamicCatalogManager, "dynamicCatalogManager is null");
        this.dynamicCatalogManager = dynamicCatalogManager;
    }

    @Override
    public String getName()
    {
        return "CREATE CATALOG";
    }

    @Override
    public ListenableFuture<?> execute(CreateCatalog statement,
                                       TransactionManager transactionManager, Metadata metadata,
                                       AccessControl accessControl,
                                       QueryStateMachine stateMachine, List<Expression> parameters)
    {
        requireNonNull(statement, "statement is null");

        final String catalogName = statement.getIdentifier().getValue();
        final Map<String, String> properties =
                statement.getCatalogElements().stream().collect(Collectors.toMap(CatalogElement::getName,
                        CatalogElement::getValue));
        dynamicCatalogManager.addCatalog(catalogName, properties);
        return immediateFuture(null);
    }
}
