/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.ExpiringCell;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.collect.Iterators;

public abstract class Selection
{
    private final Collection<ColumnDefinition> columns;
    private final ResultSet.Metadata metadata;
    private final boolean collectTimestamps;
    private final boolean collectTTLs;

    protected Selection(Collection<ColumnDefinition> columns, List<ColumnSpecification> metadata, boolean collectTimestamps, boolean collectTTLs)
    {
        this.columns = columns;
        this.metadata = new ResultSet.Metadata(metadata);
        this.collectTimestamps = collectTimestamps;
        this.collectTTLs = collectTTLs;
    }

    // Overriden by SimpleSelection when appropriate.
    public boolean isWildcard()
    {
        return false;
    }

    public ResultSet.Metadata getResultMetadata()
    {
        return metadata;
    }

    public static Selection wildcard(CFMetaData cfm)
    {
        List<ColumnDefinition> all = new ArrayList<ColumnDefinition>(cfm.allColumns().size());
        Iterators.addAll(all, cfm.allColumnsInSelectOrder());
        return new SimpleSelection(all, true);
    }

    public static Selection forColumns(Collection<ColumnDefinition> columns)
    {
        return new SimpleSelection(columns, false);
    }

    public int addColumnForOrdering(ColumnDefinition c)
    {
        columns.add(c);
        metadata.addNonSerializedColumn(c);
        return columns.size() - 1;
    }

    public boolean usesFunction(String ksName, String functionName)
    {
        return false;
    }

    private static boolean processesSelection(List<RawSelector> rawSelectors)
    {
        for (RawSelector rawSelector : rawSelectors)
        {
            if (rawSelector.processesSelection())
                return true;
        }
        return false;
    }

    public static Selection fromSelectors(CFMetaData cfm, List<RawSelector> rawSelectors) throws InvalidRequestException
    {
        List<ColumnDefinition> defs = new ArrayList<ColumnDefinition>();

        SelectorFactories factories =
                SelectorFactories.createFactoriesAndCollectColumnDefinitions(RawSelector.toSelectables(rawSelectors, cfm), cfm, defs);
        List<ColumnSpecification> metadata = collectMetadata(cfm, rawSelectors, factories);

        return processesSelection(rawSelectors) ? new SelectionWithProcessing(defs, metadata, factories)
                                                : new SimpleSelection(defs, metadata, false);
    }

    private static List<ColumnSpecification> collectMetadata(CFMetaData cfm,
                                                             List<RawSelector> rawSelectors,
                                                             SelectorFactories factories)
    {
        List<ColumnSpecification> metadata = new ArrayList<ColumnSpecification>(rawSelectors.size());
        Iterator<RawSelector> iter = rawSelectors.iterator();
        for (Selector.Factory factory : factories)
        {
            ColumnSpecification colSpec = factory.getColumnSpecification(cfm);
            ColumnIdentifier alias = iter.next().alias;
            metadata.add(alias == null ? colSpec : colSpec.withAlias(alias));
        }
        return metadata;
    }

    protected abstract Selectors newSelectors();

    /**
     * @return the list of CQL3 columns value this SelectionClause needs.
     */
    public Collection<ColumnDefinition> getColumns()
    {
        return columns;
    }

    public ResultSetBuilder resultSetBuilder(long now)
    {
        return new ResultSetBuilder(now);
    }

    public abstract boolean isAggregate();

    /**
     * Checks that selectors are either all aggregates or that none of them is.
     *
     * @param selectors the selectors to test.
     * @param messageTemplate the error message template
     * @param messageArgs the error message arguments
     * @throws InvalidRequestException if some of the selectors are aggregate but not all of them
     */
    static void validateSelectors(List<Selector> selectors, String messageTemplate, Object... messageArgs)
            throws InvalidRequestException
    {
        int aggregates = 0;
        for (Selector s : selectors)
            if (s.isAggregate())
                ++aggregates;

        if (aggregates != 0 && aggregates != selectors.size())
            throw new InvalidRequestException(String.format(messageTemplate, messageArgs));
    }

    public class ResultSetBuilder
    {
        private final ResultSet resultSet;

        /**
         * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
         * its own <code>Selectors</code> instance.
         */
        private final Selectors selectors;

        /*
         * We'll build CQL3 row one by one.
         * The currentRow is the values for the (CQL3) columns we've fetched.
         * We also collect timestamps and ttls for the case where the writetime and
         * ttl functions are used. Note that we might collect timestamp and/or ttls
         * we don't care about, but since the array below are allocated just once,
         * it doesn't matter performance wise.
         */
        List<ByteBuffer> current;
        final long[] timestamps;
        final int[] ttls;
        final long now;

        private ResultSetBuilder(long now)
        {
            this.resultSet = new ResultSet(getResultMetadata().copy(), new ArrayList<List<ByteBuffer>>());
            this.selectors = newSelectors();
            this.timestamps = collectTimestamps ? new long[columns.size()] : null;
            this.ttls = collectTTLs ? new int[columns.size()] : null;
            this.now = now;
        }

        public void add(ByteBuffer v)
        {
            current.add(v);
        }

        public void add(Cell c)
        {
            current.add(isDead(c) ? null : value(c));
            if (timestamps != null)
            {
                timestamps[current.size() - 1] = isDead(c) ? Long.MIN_VALUE : c.timestamp();
            }
            if (ttls != null)
            {
                int ttl = -1;
                if (!isDead(c) && c instanceof ExpiringCell)
                    ttl = c.getLocalDeletionTime() - (int) (now / 1000);
                ttls[current.size() - 1] = ttl;
            }
        }

        private boolean isDead(Cell c)
        {
            return c == null || !c.isLive(now);
        }

        public void newRow() throws InvalidRequestException
        {
            if (current != null)
            {
                selectors.addInputRow(this);
                if (!selectors.isAggregate())
                {
                    resultSet.addRow(selectors.getOutputRow());
                    selectors.reset();
                }
            }
            current = new ArrayList<ByteBuffer>(columns.size());
        }

        public ResultSet build() throws InvalidRequestException
        {
            if (current != null)
            {
                selectors.addInputRow(this);
                resultSet.addRow(selectors.getOutputRow());
                selectors.reset();
                current = null;
            }

            if (resultSet.isEmpty() && selectors.isAggregate())
            {
                resultSet.addRow(selectors.getOutputRow());
            }
            return resultSet;
        }

        private ByteBuffer value(Cell c)
        {
            return (c instanceof CounterCell)
                ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
                : c.value();
        }
    }

    private static interface Selectors
    {
        public boolean isAggregate();

        /**
         * Adds the current row of the specified <code>ResultSetBuilder</code>.
         *
         * @param rs the <code>ResultSetBuilder</code>
         * @throws InvalidRequestException
         */
        public void addInputRow(ResultSetBuilder rs) throws InvalidRequestException;

        public List<ByteBuffer> getOutputRow() throws InvalidRequestException;

        public void reset();
    }

    // Special cased selection for when no function is used (this save some allocations).
    private static class SimpleSelection extends Selection
    {
        private final boolean isWildcard;

        public SimpleSelection(Collection<ColumnDefinition> columns, boolean isWildcard)
        {
            this(columns, new ArrayList<ColumnSpecification>(columns), isWildcard);
        }

        public SimpleSelection(Collection<ColumnDefinition> columns, List<ColumnSpecification> metadata, boolean isWildcard)
        {
            /*
             * In theory, even a simple selection could have multiple time the same column, so we
             * could filter those duplicate out of columns. But since we're very unlikely to
             * get much duplicate in practice, it's more efficient not to bother.
             */
            super(columns, metadata, false, false);
            this.isWildcard = isWildcard;
        }

        @Override
        public boolean isWildcard()
        {
            return isWildcard;
        }

        public boolean isAggregate()
        {
            return false;
        }

        protected Selectors newSelectors()
        {
            return new Selectors()
            {
                private List<ByteBuffer> current;

                public void reset()
                {
                    current = null;
                }

                public List<ByteBuffer> getOutputRow()
                {
                    return current;
                }

                public void addInputRow(ResultSetBuilder rs) throws InvalidRequestException
                {
                    current = rs.current;
                }

                public boolean isAggregate()
                {
                    return false;
                }
            };
        }
    }

    private static class SelectionWithProcessing extends Selection
    {
        private final SelectorFactories factories;

        public SelectionWithProcessing(Collection<ColumnDefinition> columns,
                                       List<ColumnSpecification> metadata,
                                       SelectorFactories factories) throws InvalidRequestException
        {
            super(columns, metadata, factories.containsWritetimeSelectorFactory(), factories.containsTTLSelectorFactory());
            this.factories = factories;

            if (factories.doesAggregation() && !factories.containsOnlyAggregateFunctions())
                throw new InvalidRequestException("the select clause must either contains only aggregates or none");
        }

        public boolean usesFunction(String ksName, String functionName)
        {
            return factories.usesFunction(ksName, functionName);
        }

        @Override
        public int addColumnForOrdering(ColumnDefinition c)
        {
            int index = super.addColumnForOrdering(c);
            factories.addSelectorForOrdering(c, index);
            return index;
        }

        public boolean isAggregate()
        {
            return factories.containsOnlyAggregateFunctions();
        }

        protected Selectors newSelectors()
        {
            return new Selectors()
            {
                private final List<Selector> selectors = factories.newInstances();

                public void reset()
                {
                    for (int i = 0, m = selectors.size(); i < m; i++)
                    {
                        selectors.get(i).reset();
                    }
                }

                public boolean isAggregate()
                {
                    return factories.containsOnlyAggregateFunctions();
                }

                public List<ByteBuffer> getOutputRow() throws InvalidRequestException
                {
                    List<ByteBuffer> outputRow = new ArrayList<>(selectors.size());

                    for (int i = 0, m = selectors.size(); i < m; i++)
                    {
                        outputRow.add(selectors.get(i).getOutput());
                    }
                    return outputRow;
                }

                public void addInputRow(ResultSetBuilder rs) throws InvalidRequestException
                {
                    for (int i = 0, m = selectors.size(); i < m; i++)
                    {
                        selectors.get(i).addInput(rs);
                    }
                }
            };
        }

    }
}
