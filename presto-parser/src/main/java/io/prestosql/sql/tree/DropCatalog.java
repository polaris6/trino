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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DropCatalog
        extends Statement
{
    private Identifier catalogName;

    public DropCatalog(Identifier catalogName)
    {
        this(Optional.empty(), catalogName);
    }

    public DropCatalog(NodeLocation location, Identifier catalogName)
    {
        this(Optional.of(location), catalogName);
    }

    private DropCatalog(Optional<NodeLocation> location, Identifier catalogName)
    {
        super(location);
        this.catalogName = catalogName;
    }

    public Identifier getCatalogName()
    {
        return catalogName;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropCatalog(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DropCatalog that = (DropCatalog) o;
        return Objects.equals(catalogName, that.catalogName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .toString();
    }
}
