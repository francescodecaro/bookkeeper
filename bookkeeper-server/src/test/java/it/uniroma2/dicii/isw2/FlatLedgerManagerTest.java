package it.uniroma2.dicii.isw2;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//@RunWith(Parameterized.class)
public class FlatLedgerManagerTest {
//
//    long ledgerId;
//
//    @Parameterized.Parameters
//    public static Collection params() {
//        return Arrays.asList(new Object[][]{
//                { 3 }
//        });
//    }
//
//    public FlatLedgerManagerTest(int bookies) {
//        super(bookies);
//    }
//
//    @Before()
//    public void configure() throws Exception {
//        ledgerId = bkc.createLedger(3, 2, 1, BookKeeper.DigestType.MAC, "".getBytes()).getId();
//        Versioned<LedgerMetadata> ledgerMetadataVersioned = bkc.getLedgerManager().readLedgerMetadata(ledgerId).get();
//        System.out.println(ledgerMetadataVersioned.getValue().getDigestType());
//    }
//
//    @Test
//    public void testLedgerPath() {
//        assertTrue(true);
//    }
//
//    @Test
//    public void testCreate() {
//        bkc.getLedgerManager().createLedgerMetadata(ledgerId, new LedgerMetadata() {
//            @Override
//            public long getLedgerId() {
//                return 0;
//            }
//
//            @Override
//            public int getEnsembleSize() {
//                return 0;
//            }
//
//            @Override
//            public int getWriteQuorumSize() {
//                return 0;
//            }
//
//            @Override
//            public int getAckQuorumSize() {
//                return 0;
//            }
//
//            @Override
//            public long getLastEntryId() {
//                return 0;
//            }
//
//            @Override
//            public long getLength() {
//                return 0;
//            }
//
//            @Override
//            public boolean hasPassword() {
//                return false;
//            }
//
//            @Override
//            public byte[] getPassword() {
//                return new byte[0];
//            }
//
//            @Override
//            public DigestType getDigestType() {
//                return null;
//            }
//
//            @Override
//            public long getCtime() {
//                return 0;
//            }
//
//            @Override
//            public boolean isClosed() {
//                return false;
//            }
//
//            @Override
//            public Map<String, byte[]> getCustomMetadata() {
//                return null;
//            }
//
//            @Override
//            public List<BookieId> getEnsembleAt(long entryId) {
//                return null;
//            }
//
//            @Override
//            public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
//                return null;
//            }
//
//            @Override
//            public State getState() {
//                return null;
//            }
//
//            @Override
//            public String toSafeString() {
//                return null;
//            }
//
//            @Override
//            public int getMetadataFormatVersion() {
//                return 0;
//            }
//
//            @Override
//            public long getCToken() {
//                return 0;
//            }
//        });
//    }
}
