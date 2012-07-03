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
            // XXX: What is getMeanColumns? #see DataTracker.getMeanColumns
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
        logger.info("A test1");
        System.out.println("a test2");
        System.err.println("a test2");
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause());
        final SecondaryIndex index = indexManager.getIndexForColumn(primary.column_name);
        if (logger.isDebugEnabled())
            logger.debug("Primary scan clause is " + baseCfs.getComparator().getString(primary.column_name));
        assert index != null;
        // primary.column_name = "birth_date", primary.value = ? (not UTF8)
        // indexKey = "DecoratedKey(1973, 00000000000007b5)"
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
                // XXX: What exactly does rowsPerQuery mean here?
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
                        // XXX: likely here, (though maybe above -- maybe even above
                        // isMarkedForDelete()?)
                        //
                        // How does iterating columns relate to a new Row() below??
                        // A 'row' is really just a key to an entry in a ColumnFamily i
                        // suppose. a 'Row' is probably a deferred lookup for later then?

                        // It seems like CassandraServer will just iterate through a
                        // ColumnFamily (ie thriftifyKeySlices just calls
                        // thriftifyColumnFamily). Is a CF in that case a subset of a real
                        // CF?
                        //
                        // It's really time for some test code so I can analyze what all
                        // this stuff is exactly doing.

                        // So it looks like CFS.getColumnFamily takes a filter (see
                        // below). So it would actually return a CF that is filtered
                        // (subset).
                        //
                        // double-check the rows returned by the index to make sure the
                        // column value still matches the indexed one; if it does not,
                        // delete the index entry (with timestamp of original insert, so a
                        // new update w/ the same value doesn't get clobbered) so we don't
                        // keep making the same mistake

                        // XXX: Column's name is 'prothfuss'.. value is an empty byetbuffer
                        // users.users_birth_date_idx should then look like this:
                        // "users.users_birth_date_idx" {
                        //     "1973" {
                        //         "prothfuss": null,
                        //         "jjenvey": null,
                        //     }
                        // }

                        logger.info("A test");
                        logger.debug("Returning index hit for {}", dk);
                        ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, path, filter.initialFilter()));
                        // While the column family we'll get in the end should contains the primary clause column, the initialFilter may not have found it and can thus be null
                        if (data == null)
                            data = ColumnFamily.create(baseCfs.metadata);
                        IColumn indexedColumn = data.getColumn(primary.column_name);
                        if (!primary.value.equals(indexedColumn.value())) {
                            // This index entry is stale: delete it
                            // XXX: getIndexCfs().table.name maybe -> getIndexCfs().getColumnFamilyName()
                            RowMutation rm = new RowMutation(index.getIndexCfs().table.name, primary.value);
                            rm.delete(new QueryPath(index.getIndexCfs().table.name, null, column.name()),
                                      indexedColumn.timestamp());
                            try {
                                rm.apply(); // hrmm
                            } catch (IOException ioe) {
                                throw new RuntimeException(); // XXX:
                            }
                        }
                        
                        // XXX: somehow look at data and consider if primary.column_name
                        // is still equal. if not, we have to delete the index entry (With
                        // timestamp of original insert)
                        //if (data.getColumn(primary.column_name)
                        // if (isStaleIndex(primary.column_name, data)) {
                        //     delete from the index..
                        //     continue
                        
                        return new Row(dk, data);
                    }
                 }
             }

            public void close() throws IOException {}
        };
    }
}
