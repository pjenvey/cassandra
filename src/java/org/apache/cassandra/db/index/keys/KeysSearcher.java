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
package org.apache.cassandra.db.index.keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeysSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);

    public KeysSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    private IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        for (IndexExpression expression : clause)
        {
            //skip columns belonging to a different index type
            if(!columns.contains(expression.column_name))
                continue;

            SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
            if (index == null || (expression.op != IndexOperator.EQ))
                continue;
            int columns = index.getIndexCfs().getMeanColumns();
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }
        return best;
    }

    private String expressionString(IndexExpression expr)
    {
        return String.format("'%s.%s %s %s'",
                             baseCfs.columnFamily,
                             baseCfs.getComparator().getString(expr.column_name),
                             expr.op,
                             baseCfs.metadata.getColumn_metadata().get(expr.column_name).getValidator().getString(expr.value));
    }

    public boolean isIndexing(List<IndexExpression> clause)
    {
        return highestSelectivityPredicate(clause) != null;
    }

    @Override
    public List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IFilter dataFilter, boolean maxIsColumns)
    {
        assert clause != null && !clause.isEmpty();
        ExtendedFilter filter = ExtendedFilter.create(baseCfs, dataFilter, clause, maxResults, maxIsColumns, false);
        return baseCfs.filter(getIndexedIterator(range, filter), filter);
    }

    public ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final AbstractBounds<RowPosition> range, final ExtendedFilter filter)
    {
        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause());
        final SecondaryIndex index = indexManager.getIndexForColumn(primary.column_name);
        if (logger.isDebugEnabled())
            logger.debug("Primary scan clause is " + baseCfs.getComparator().getString(primary.column_name));
        assert index != null;
        final DecoratedKey indexKey = indexManager.getIndexKeyFor(primary.column_name, primary.value);

        /*
         * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
         * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the small
         * possible key having a given token. A fix would be to actually store the token along the key in the
         * indexed row.
         */
        final ByteBuffer startKey = range.left instanceof DecoratedKey ? ((DecoratedKey)range.left).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        final ByteBuffer endKey = range.right instanceof DecoratedKey ? ((DecoratedKey)range.right).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            private ByteBuffer lastSeenKey = startKey;
            private Iterator<IColumn> indexColumns;
            private final QueryPath path = new QueryPath(baseCfs.columnFamily);
            private int columnsRead = Integer.MAX_VALUE;

            protected Row computeNext()
            {
                int meanColumns = Math.max(index.getIndexCfs().getMeanColumns(), 1);
                // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
                int rowsPerQuery = Math.max(Math.min(filter.maxRows(), filter.maxColumns() / meanColumns), 2);
                while (true)
                {
                    if (indexColumns == null || !indexColumns.hasNext())
                    {
                        if (columnsRead < rowsPerQuery)
                        {
                            logger.debug("Read only {} (< {}) last page through, must be done", columnsRead, rowsPerQuery);
                            return endOfData();
                        }

                        if (logger.isDebugEnabled())
                            logger.debug("Scanning index {} starting with {}",
                                         expressionString(primary), index.getBaseCfs().metadata.getKeyValidator().getString(startKey));

                        QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                             new QueryPath(index.getIndexCfs().getColumnFamilyName()),
                                                                             lastSeenKey,
                                                                             endKey,
                                                                             false,
                                                                             rowsPerQuery);
                        ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                        logger.debug("fetched {}", indexRow);
                        if (indexRow == null)
                        {
                            logger.debug("no data, all done");
                            return endOfData();
                        }

                        Collection<IColumn> sortedColumns = indexRow.getSortedColumns();
                        columnsRead = sortedColumns.size();
                        indexColumns = sortedColumns.iterator();
                        IColumn firstColumn = sortedColumns.iterator().next();

                        // Paging is racy, so it is possible the first column of a page is not the last seen one.
                        if (lastSeenKey != startKey && lastSeenKey.equals(firstColumn.name()))
                        {
                            // skip the row we already saw w/ the last page of results
                            indexColumns.next();
                            logger.debug("Skipping {}", baseCfs.metadata.getKeyValidator().getString(firstColumn.name()));
                        }
                        else if (range instanceof Range && indexColumns.hasNext() && firstColumn.name().equals(startKey))
                        {
                            // skip key excluded by range
                            indexColumns.next();
                            logger.debug("Skipping first key as range excludes it");
                        }
                    }

                    while (indexColumns.hasNext())
                    {
                        IColumn column = indexColumns.next();
                        lastSeenKey = column.name();
                        if (column.isMarkedForDelete())
                        {
                            logger.debug("skipping {}", column.name());
                            continue;
                        }

                        DecoratedKey dk = baseCfs.partitioner.decorateKey(lastSeenKey);
                        if (!range.right.isMinimum(baseCfs.partitioner) && range.right.compareTo(dk) < 0)
                        {
                            logger.debug("Reached end of assigned scan range");
                            return endOfData();
                        }
                        if (!range.contains(dk))
                        {
                            logger.debug("Skipping entry {} outside of assigned scan range", dk.token);
                            continue;
                        }

                        logger.debug("Returning index hit for {}", dk);
                        ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, path, filter.initialFilter()));
                        // While the column family we'll get in the end should contains the primary clause column, the initialFilter may not have found it and can thus be null
                        if (data == null)
                            data = ColumnFamily.create(baseCfs.metadata);
                        IColumn indexedColumn = data.getColumn(primary.column_name);
                        /*
                        try {
                        logger.debug("!! name: {} value: {}", org.apache.cassandra.utils.ByteBufferUtil.string(indexedColumn.name()), org.apache.cassandra.utils.ByteBufferUtil.string(indexedColumn.value()));
                        logger.debug("!! cf marked: {} column marked: {}", data.isMarkedForDelete(), indexedColumn.isMarkedForDelete());
                        } catch (java.nio.charset.CharacterCodingException cce) { throw new RuntimeException(cce);}
                        */

                        if (indexedColumn == null || !primary.value.equals(indexedColumn.value()))
                        {
                            if (logger.isDebugEnabled())
                                logger.debug("Updating stale index entry for {} {}", indexKey, column.name());
                            if (logger.isDebugEnabled())
                                logger.debug("indexedColumn: {}", indexedColumn);

                            try
                            {
                                // XXX: deleteFromIndexes does a couple unnecessary lookups (of data we already
                                // have on hand)
                                //indexManager.deleteFromIndexes(dk, Arrays.asList(indexedColumn));
                                indexManager.deleteFromIndexes(dk, Arrays.asList(column));
                                // XXX: refactor this cut&paste out of SecondaryIndexManager.applyIndexUpdates
                                /*
                                if (index instanceof org.apache.cassandra.db.index.PerRowSecondaryIndex)
                                {
                                    throw new RuntimeException("Not Implemented");
                                }
                                else
                                {
                                    // XXX: indexedColumn can be null
                                    ((org.apache.cassandra.db.index.PerColumnSecondaryIndex) index).deleteColumn(indexKey, column.name(), column);
                                }
                                */

                            }
                            catch (IOException ioe)
                            {
                                throw new RuntimeException(ioe);
                            }
                            continue;
                        }
                        return new Row(dk, data);
                    }
                 }
             }

            public void close() throws IOException {}
        };
    }
}
