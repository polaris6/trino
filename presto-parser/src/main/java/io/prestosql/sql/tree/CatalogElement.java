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

public final class CatalogElement
        extends Node
{
    private String name;
    private String value;

    public CatalogElement(String name, String value)
    {
        this(Optional.empty(), name, value);
    }

    public CatalogElement(NodeLocation location, String name, String value)
    {
        this(Optional.of(location), name, value);
    }

    private CatalogElement(Optional<NodeLocation> location, String name, String value)
    {
        super(location);
        this.name = name;
        this.value = value;
    }

    public String getName()
    {
        return name;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitNode(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
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
        CatalogElement that = (CatalogElement) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .toString();
    }
}
