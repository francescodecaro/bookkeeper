package it.uniroma2.dicii.isw2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class WriteCacheTest {

    private static final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    private static final int ENTRY_SIZE = 1024;
    private static final int MAX_ENTRIES = 10;
    private static final int MAX_CACHE_SIZE = MAX_ENTRIES * ENTRY_SIZE;

    private WriteCache writeCache;


    private ByteBuf entry;
    private int ledgerId;
    private int entryId;
    private int nEntries;
    private ByteBuf failEntry;
    private int expectedCount;
    private int expectedSize;
    private int maxSegmentSize;

    @Parameterized.Parameters
    public static Collection params() {
        ByteBuf entry = allocator.buffer(ENTRY_SIZE);
        entry.writerIndex(entry.capacity());

        ByteBuf failEntry = allocator.buffer(ENTRY_SIZE * (MAX_ENTRIES + 1));
        failEntry.writerIndex(failEntry.capacity());

        // ledgerId, entryId, entry
        // ledgerId: <0, >= 0
        // entryId: <0, >= 0
        // entry: valid, invalid, null
        // ledgerId: -1, 0
        // entryId: -1, 0
        // entry: valid, invalid, null
        return Arrays.asList(new Object[][]{
                // minimal test suite
                { entry, -1, -1, 1, null, 0, 0, IllegalArgumentException.class, 0 },
                { entry, 0, 0, 0, failEntry, 0, 0, null, 0 },
                { null, 0, 0, 1, null, 0, 0, NullPointerException.class, 0 },

                // success test
                { entry, 0, 0, 1, null, 0, 0, null, 0 },

                // Added after coverage
                { entry, 0, 0, 1, null, 0, 0, IllegalArgumentException.class, -1 * 1024 * 1024 * 1024},
                { entry, 0, 0, 1, null, 0, 0, IllegalArgumentException.class, 1000 * 1000 * 1000}, // not aligned to power of two
                { entry, 0, 0, 1, null, 0, 0, null, 1 * 1024},


//                { entry, 0, 0, 1, null, 1, ENTRY_SIZE },
//                { entry, 0, 0, 0, failEntry, 0, 0 },
//                { entry, 0, 0, 5, null, 5, ENTRY_SIZE * 5 },

        });
    }

    public WriteCacheTest(ByteBuf entry, int ledgerId, int entryId, int nEntries, ByteBuf failEntry,
                          int expectedCount, int expectedSize, Class<? extends Exception> expectedException,
                          int maxSegmentSize) {
        this.entry = entry;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.nEntries = nEntries;
        this.failEntry = failEntry;
        this.expectedCount = expectedCount;
        this.expectedSize = expectedSize;
        this.maxSegmentSize = maxSegmentSize;

        if (expectedException != null) this.expectedException.expect(expectedException);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void configure() {
        writeCache = maxSegmentSize != 0 ? new WriteCache(allocator, MAX_CACHE_SIZE, maxSegmentSize) : new WriteCache(allocator, MAX_CACHE_SIZE);
    }

    @After
    public void tearDown() {
        if (writeCache != null) {
            writeCache.clear();
            writeCache.close();
        }
    }

    @Test
    public void testPut() {
        int entries = nEntries + entryId;
        for (int i = entryId; i < entries; i++) {
            assertTrue(writeCache.put(ledgerId, i, entry));
            assertEquals(entry, writeCache.get(ledgerId, i));
        }

        assertNull(writeCache.get(ledgerId, entries + 1));

        if (failEntry != null) {
            assertFalse(writeCache.put(ledgerId, entries + 1, failEntry));
        }
        assertEquals(nEntries, writeCache.count());
        assertEquals(nEntries *  ENTRY_SIZE, writeCache.size());


        if (nEntries == 0) {
            assertTrue(writeCache.isEmpty());
        }
    }

    @Test
    public void testDeleteLedger() {
        int entries = nEntries + entryId;
        for (int i = entryId; i < entries; i++) {
            assertTrue(writeCache.put(ledgerId, i, entry));
        }

        if (nEntries > 0) {
            assertFalse(writeCache.isEmpty());
        }
        writeCache.deleteLedger(ledgerId);

        assertTrue(writeCache.put(ledgerId + 1, 0, entry));

        writeCache.forEach((lId, entryId, entry1) -> assertNotEquals(lId, ledgerId));
    }


    @Test
    public void testLastEntry() {
        int entries = nEntries + entryId;
        for (int i = entryId; i < entries; i++) {
            assertTrue(writeCache.put(ledgerId, i, entry));
        }

        assertEquals(nEntries > 0 ? entry : null, writeCache.getLastEntry(ledgerId));
    }

//    @Test
//    public void testPut() {
//        ByteBuf entry = allocator.buffer(entrySize);
//        entry.writerIndex(entry.capacity());
//
//        writeCache.put(0L, 0L, entry);
//        assertEquals(1, writeCache.count());
//        assertEquals(1 *  entrySize, writeCache.size());
//    }
//
//
//    @Test
//    public void testPutMultipleEntries() {
//
//        ByteBuf entry = allocator.buffer(entrySize);
//        entry.writerIndex(entry.capacity());
//
//        int entries = maxEntries / 2;
//        for (int i = 0; i < entries; i++) {
//            assertTrue(writeCache.put(0L, i, entry));
//        }
//
//        assertEquals(entries, writeCache.count());
//        assertEquals(entries *  entrySize, writeCache.size());
//    }

//    @Test
//    public void testPutCacheFull() {
//
//        ByteBuf entry = allocator.buffer(entrySize);
//        entry.writerIndex(entry.capacity());
//
//        int entries = maxEntries;
//        for (int i = 0; i < entries; i++) {
//            assertTrue(writeCache.put(0L, i, entry));
//        }
//
//        assertFalse(writeCache.put(0L, entries + 1, entry));
//        assertEquals(maxEntries, writeCache.count());
//        assertEquals(entries *  entrySize, writeCache.size());
//    }

//    @Test
//    public void testPutEntryBiggerThanMaxCacheSize() {
//
//        ByteBuf entry = allocator.buffer(entrySize * (maxEntries + 1));
//        entry.writerIndex(entry.capacity());
//
//        assertFalse(writeCache.put(0L, 0L, entry));
//        assertEquals(0, writeCache.count());
//        assertEquals(0, writeCache.size());
//    }
}
