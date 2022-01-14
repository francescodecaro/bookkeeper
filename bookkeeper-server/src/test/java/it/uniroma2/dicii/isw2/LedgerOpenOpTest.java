package it.uniroma2.dicii.isw2;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.Enumeration;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class LedgerOpenOpTest extends BookKeeperClusterTestCase {

    LedgerHandle lh;
    long ledgerId;


    public LedgerOpenOpTest() {
        super(3);
    }

    @Before
    public void configure() throws BKException, InterruptedException {
        lh = bkc.createLedger(3, 2, 1, BookKeeper.DigestType.CRC32, "".getBytes());
        ledgerId = lh.getId();
    }

    @Test
    public void testOpen() {
        try {
            ReadHandle rh = bkc.openLedger(ledgerId, BookKeeper.DigestType.MAC, "".getBytes());
            System.out.println(rh.getLedgerMetadata().getDigestType());
            assertTrue(true);
        } catch (BKException e) {
            fail();
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testOpenWrongLedgerId() {
        try {
            bkc.openLedger(-1L, BookKeeper.DigestType.CRC32, "".getBytes());
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.NoSuchLedgerExistsOnMetadataServerException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }
    }


//    @Test
//    public void testOpenAfterClosed() {
//        try {
//            lh.close();
//            bkc.openLedger(ledgerId, BookKeeper.DigestType.CRC32, "".getBytes());
//        } catch (InterruptedException e) {
//            fail();
//        } catch (BKException e) {
//            assertEquals(BKException.Code.LedgerClosedException, e.getCode());
//        }
//    }


    @Test
    public void testOpenWrongDigest() {
        try {
            ReadHandle rh = bkc.openLedger(ledgerId, BookKeeper.DigestType.MAC, "".getBytes());
            assertEquals(DigestType.CRC32, rh.getLedgerMetadata().getDigestType());
        } catch (BKException e) {
            fail();
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testOpenWithoutDigestAutodetection() {
        try {
            bkc.getConf().setEnableDigestTypeAutodetection(false);
            bkc.openLedger(ledgerId, BookKeeper.DigestType.MAC, "".getBytes());
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.DigestMatchException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testOpenWithoutDigestAutodetection2() {
        try {
            bkc.getConf().setEnableDigestTypeAutodetection(false);
            bkc.newOpenLedgerOp().withLedgerId(ledgerId).withPassword("".getBytes()).execute().get();
            assertTrue(true);
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            fail();
        }
    }

    @Test
    public void testOpenWithoutDigestAutodetection3() {
        try {
            bkc.getConf().setEnableDigestTypeAutodetection(false);
            ReadHandle rh = bkc.newOpenLedgerOp().withLedgerId(ledgerId).withPassword("".getBytes()).withDigestType(DigestType.MAC).execute().get();
            fail();
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof BKException)){
                fail();
            }
            BKException cause = (BKException) e.getCause();
            assertEquals(BKException.Code.DigestMatchException, cause.getCode());
        }
    }

    @Test
    public void testOpenWrongPassword() {
        try {
            bkc.openLedger(ledgerId, BookKeeper.DigestType.CRC32, "pass".getBytes());
        } catch (BKException e) {
            assertEquals(BKException.Code.UnauthorizedAccessException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testOpenDeletedLedger() {
        try {
            bkc.deleteLedger(ledgerId);
            bkc.openLedger(ledgerId, BookKeeper.DigestType.CRC32, "".getBytes());
            fail();

        } catch (InterruptedException e) {
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.NoSuchLedgerExistsOnMetadataServerException, e.getCode());
        }
    }

    @Test
    public void testOpenNoRecovery() {
        try {
            LedgerHandle lh2 = bkc.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, "".getBytes());
            long entry = lh.addEntry("test".getBytes());

            Enumeration<LedgerEntry> entries = lh2.readUnconfirmedEntries(0, entry);

            int count = 0;
            while (entries.hasMoreElements()) {
                count++;
                System.out.println(new String(entries.nextElement().getEntry()));
            }

            assertEquals(1, count);
        } catch (InterruptedException e) {
            fail();
        } catch (BKException e) {
            fail();
        }
    }

    @Test
    public void testOpenBuilder() {
        try {
            bkc.newOpenLedgerOp()
                    .withLedgerId(ledgerId)
                    .withPassword("".getBytes())
                    .withDigestType(DigestType.CRC32)
                    .withRecovery(false)
                    .execute()
                    .get();
            assertTrue(true);
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            fail();
        }
    }

}
