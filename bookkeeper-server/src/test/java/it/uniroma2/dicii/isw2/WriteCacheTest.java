package it.uniroma2.dicii.isw2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

        ByteBuf entrySmaller = allocator.buffer(512);
        entrySmaller.writerIndex(entrySmaller.capacity());

        ByteBuf failEntry = allocator.buffer(ENTRY_SIZE * (MAX_ENTRIES + 1));
        failEntry.writerIndex(failEntry.capacity());

        ByteBuf entry2 = allocator.buffer(ENTRY_SIZE * 3);
        entry2.writerIndex(entry2.capacity());

        // ledgerId, entryId, entry
        // ledgerId: <0, >= 0
        // entryId: <0, >= 0
        // entry: valid, invalid, null
        // ledgerId: -1, 0
        // entryId: -1, 0
        // entry: valid, invalid, null
        return Arrays.asList(new Object[][]{
                // minimal test suite
                { entry, -1, -1, 1, null, 0, 0, IllegalArgumentException.class, -1, null },
                { entry, 0, 0, 0, failEntry, 0, 0, null, -1, null  },
                { null, 0, 0, 1, null, 0, 0, NullPointerException.class, -1, null  },

                // success test
                { entry, 0, 0, 1, null, 0, 0, null, -1, null },

//                // Added after coverage
                { entry, 0, 0, 1, null, 0, 0, null, 0, "Max segment size needs to be in form of 2^n" },
                { entry, 0, 0, 1, null, 0, 0, IllegalArgumentException.class, -1 * 1024 * 1024 * 1024, null },
                { entry, 0, 0, 1, null, 0, 0, IllegalArgumentException.class, 1000 * 1000 * 1000, null }, // not aligned to power of two
                { entry, 0, 0, 1, null, 0, 0, null, 1 * 1024, null },

                { entry, 0, 0, 3, entry2, 0, 0, null, 2048, null },


                { entry, 0, 0, 10, null, 0, 0, null, -1, null },


        });
    }

    public WriteCacheTest(ByteBuf entry, int ledgerId, int entryId, int nEntries, ByteBuf failEntry,
                          int expectedCount, int expectedSize, Class<? extends Exception> expectedException,
                          int maxSegmentSize, String notExpectedExceptionMessage) {
        this.entry = entry;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.nEntries = nEntries;
        this.failEntry = failEntry;
        this.expectedCount = expectedCount;
        this.expectedSize = expectedSize;
        this.maxSegmentSize = maxSegmentSize;

        if (expectedException != null) this.expectedException.expect(expectedException);
        if (notExpectedExceptionMessage != null) {
            this.expectedException.expectMessage(new BaseMatcher<String>() {
                @Override
                public void describeTo(Description description) {

                }

                @Override
                public boolean matches(Object o) {
                    if (o == null) return true;
                    String s = (String) o;
                    return !s.equals(notExpectedExceptionMessage);
                }

                @Override
                public void describeMismatch(Object o, Description description) {

                }
            });
        }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void configure() {
        writeCache = maxSegmentSize != -1 ? new WriteCache(allocator, MAX_CACHE_SIZE, maxSegmentSize) : new WriteCache(allocator, MAX_CACHE_SIZE);
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
        System.out.println("testPut");
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
        assertEquals(nEntries * ENTRY_SIZE, writeCache.size());


        if (nEntries == 0) {
            assertTrue(writeCache.isEmpty());
        }
    }

    @Test
    public void testDeleteLedger() {
        System.out.println("testDeleteLedger");

        int entries = nEntries + entryId;
        for (int i = entryId; i < entries; i++) {
            assertTrue(writeCache.put(ledgerId, i, entry));
        }

        if (nEntries > 0) {
            assertFalse(writeCache.isEmpty());
        }
        writeCache.deleteLedger(ledgerId);

        if (nEntries < MAX_ENTRIES) {
            assertTrue(writeCache.put(ledgerId + 1, 0, entry));
        }

        writeCache.forEach((lId, entryId, entry1) -> assertNotEquals(lId, ledgerId));
    }


    @Test
    public void testLastEntry() {
        System.out.println("testLastEntry");
        int entries = nEntries + entryId;
        for (int i = entryId; i < entries; i++) {
            assertTrue(writeCache.put(ledgerId, i, entry));
        }

        assertEquals(nEntries > 0 ? entry : null, writeCache.getLastEntry(ledgerId));
    }
}
