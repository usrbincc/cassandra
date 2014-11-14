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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.db.compaction.LazilyCompactedRow;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SSTableRewriterTest extends SchemaLoader
{
    private static final String KEYSPACE = "SSTableRewriterTest";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
    }


    @Test
    public void basicTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        for (int j = 0; j < 100; j ++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            Mutation rm = new Mutation(KEYSPACE, key);
            rm.add(CF, Util.cellname("0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        SSTableRewriter writer = new SSTableRewriter(cfs, sstables, 1000, false);
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables);)
        {
            ICompactionScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while(scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                writer.append(row);
            }
        }
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, writer.finish(), OperationType.COMPACTION);

        validateCFS(cfs);

    }


    @Test
    public void testFileRemoval() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < 100; i++)
            cf.addColumn(Util.cellname(i), ByteBuffer.allocate(1000), 1);
        File dir = cfs.directories.getDirectoryForNewSSTables();
        SSTableWriter writer = getWriter(cfs, dir);
        for (int i = 0; i < 500; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        SSTableReader s = writer.openEarly(1000);
        assertFileCounts(dir.list(), 2, 3);
        for (int i = 500; i < 1000; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        SSTableReader s2 = writer.openEarly(1000);
        assertTrue(s != s2);
        assertFileCounts(dir.list(), 2, 3);
        s.markObsolete();
        s.releaseReference();
        Thread.sleep(1000);
        assertFileCounts(dir.list(), 0, 3);
        writer.abort(false);
        Thread.sleep(1000);
        assertFileCounts(dir.list(), 0, 0);
        validateCFS(cfs);
    }

    @Test
    public void testFileRemovalNoAbort() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < 1000; i++)
            cf.addColumn(Util.column(String.valueOf(i), "a", 1));
        File dir = cfs.directories.getDirectoryForNewSSTables();
        SSTableWriter writer = getWriter(cfs, dir);

        for (int i = 0; i < 500; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        SSTableReader s = writer.openEarly(1000);
        //assertFileCounts(dir.list(), 2, 3);
        for (int i = 500; i < 1000; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        writer.closeAndOpenReader();
        s.markObsolete();
        s.releaseReference();
        Thread.sleep(1000);
        assertFileCounts(dir.list(), 0, 0);
        validateCFS(cfs);
    }


    @Test
    public void testNumberOfFilesAndSizes() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long startStorageMetricsLoad = StorageMetrics.load.count();
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    assertEquals(s.bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.count());
                    assertEquals(s.bytesOnDisk(), cfs.metric.totalDiskSpaceUsed.count());

                }
            }
        }
        List<SSTableReader> sstables = rewriter.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        long sum = 0;
        for (SSTableReader x : cfs.getSSTables())
            sum += x.bytesOnDisk();
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.count());
        assertEquals(startStorageMetricsLoad - s.bytesOnDisk() + sum, StorageMetrics.load.count());
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        Thread.sleep(1000);
        // tmplink and tmp files should be gone:
        assertEquals(sum, cfs.metric.totalDiskSpaceUsed.count());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_dont_clean_readers() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }
        }
        List<SSTableReader> sstables = rewriter.finish();
        assertEquals(files, sstables.size());
        assertEquals(files + 1, cfs.getSSTables().size());
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        assertEquals(files, cfs.getSSTables().size());
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }


    @Test
    public void testNumberOfFiles_abort() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long startSize = cfs.metric.liveDiskSpaceUsed.count();
        DecoratedKey origFirst = s.first;
        DecoratedKey origLast = s.last;
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }
        }
        rewriter.abort();
        Thread.sleep(1000);
        assertEquals(startSize, cfs.metric.liveDiskSpaceUsed.count());
        assertEquals(1, cfs.getSSTables().size());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        assertEquals(cfs.getSSTables().iterator().next().first, origFirst);
        assertEquals(cfs.getSSTables().iterator().next().last, origLast);
        validateCFS(cfs);

    }

    @Test
    public void testNumberOfFiles_abort2() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        DecoratedKey origFirst = s.first;
        DecoratedKey origLast = s.last;
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
                if (files == 3)
                {
                    //testing to abort when we have nothing written in the new file
                    rewriter.abort();
                    break;
                }
            }
        }
        Thread.sleep(1000);
        assertEquals(1, cfs.getSSTables().size());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);

        assertEquals(cfs.getSSTables().iterator().next().first, origFirst);
        assertEquals(cfs.getSSTables().iterator().next().last, origLast);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_finish_empty_new_writer() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
                if (files == 3)
                {
                    //testing to finish when we have nothing written in the new file
                    List<SSTableReader> sstables = rewriter.finish();
                    cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
                    break;
                }
            }
        }
        Thread.sleep(1000);
        assertEquals(files - 1, cfs.getSSTables().size()); // we never wrote anything to the last file
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_truncate() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }
        }
        List<SSTableReader> sstables = rewriter.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        cfs.truncateBlocking();
        validateCFS(cfs);
    }

    @Test
    public void testSmallFiles() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        SSTableReader s = writeFile(cfs, 400);
        DecoratedKey origFirst = s.first;
        cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(1000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ICompactionScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 2500000)
                {
                    assertEquals(1, cfs.getSSTables().size()); // we dont open small files early ...
                    assertEquals(origFirst, cfs.getSSTables().iterator().next().first); // ... and the first key should stay the same
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                }
            }
        }
        List<SSTableReader> sstables = rewriter.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < count / 100; i++)
            cf.addColumn(Util.cellname(i), ByteBuffer.allocate(1000), 1);
        File dir = cfs.directories.getDirectoryForNewSSTables();
        String filename = cfs.getTempSSTablePath(dir);

        SSTableWriter writer = SSTableWriter.create(filename, 0, 0);

        for (int i = 0; i < count * 5; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        return writer.closeAndOpenReader();
    }

    private void validateCFS(ColumnFamilyStore cfs)
    {
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.referenceCount());
        }
        assertTrue(cfs.getDataTracker().getCompacting().isEmpty());
    }


    private void assertFileCounts(String [] files, int expectedtmplinkCount, int expectedtmpCount)
    {
        int tmplinkcount = 0;
        int tmpcount = 0;
        for (String f : files)
        {
            if (f.contains("tmplink-"))
                tmplinkcount++;
            if (f.contains("tmp-"))
                tmpcount++;
        }
        assertEquals(expectedtmplinkCount, tmplinkcount);
        assertEquals(expectedtmpCount, tmpcount);
    }

    private SSTableWriter getWriter(ColumnFamilyStore cfs, File directory)
    {
        String filename = cfs.getTempSSTablePath(directory);
        return SSTableWriter.create(filename, 0, 0);
    }
}
