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

public class CreateCatalog
        extends Statement
{
    private final Identifier identifier;
    private final List<CatalogElement> catalogElements;

    public CreateCatalog(Identifier identifier, List<CatalogElement> catalogElements)
    {
        this(Optional.empty(), identifier, catalogElements);
    }

    public CreateCatalog(NodeLocation location, Identifier identifier, List<CatalogElement> catalogElements)
    {
        this(Optional.of(location), identifier, catalogElements);
    }

    private CreateCatalog(Optional<NodeLocation> location, Identifier identifier, List<CatalogElement> catalogElements)
    {
        super(location);
        this.identifier = identifier;
        this.catalogElements = catalogElements;
    }

    public Identifier getIdentifier()
    {
        return identifier;
    }

    public List<CatalogElement> getCatalogElements()
    {
        return catalogElements;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateCatalog(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("identifier", identifier)
                .add("catalogElements", catalogElements)
                .toString();
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
        CreateCatalog that = (CreateCatalog) o;
        return Objects.equals(identifier, that.identifier) &&
                Objects.equals(catalogElements, that.catalogElements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identifier, catalogElements);
    }
}
