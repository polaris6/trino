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
import io.prestosql.sql.tree.DropCatalog;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

public class DropCatalogTask
        implements DataDefinitionTask<DropCatalog>
{
    private DynamicCatalogManager dynamicCatalogManager;

    @Inject
    public DropCatalogTask(DynamicCatalogManager dynamicCatalogManager)
    {
        this.dynamicCatalogManager = dynamicCatalogManager;
    }

    @Override
    public String getName()
    {
        return "DROP CATALOG";
    }

    @Override
    public ListenableFuture<?> execute(DropCatalog statement,
                                       TransactionManager transactionManager, Metadata metadata,
                                       AccessControl accessControl,
                                       QueryStateMachine stateMachine, List<Expression> parameters)
    {
        requireNonNull(statement, "statement is null");
        dynamicCatalogManager.deleteCatalog(statement.getCatalogName().getValue());
        return immediateFuture(null);
    }
}
