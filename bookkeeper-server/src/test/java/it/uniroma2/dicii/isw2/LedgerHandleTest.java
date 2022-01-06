package it.uniroma2.dicii.isw2;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.TestTimedOutException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LedgerHandleTest extends BookKeeperClusterTestCase {

    final DigestType digestType;
    final byte[] testPasswd = "".getBytes();

    private byte[] entry;
    private int ensembleSize;
    private int writeQuorum;
    private int ackQuorum;
    private Map<String, byte[]> customeMetadata;
    private BKException exception;


    @Parameterized.Parameters
    public static Collection params() {
        DigestType digestType = DigestType.CRC32;
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("m1-key", "m1-value".getBytes());
        customMetadata.put("m2-key", "m2-value".getBytes());
        return Arrays.asList(new Object[][]{
                { 3, "test".getBytes(), 3, 2, 1, digestType, customMetadata, null },
//                { 3, "test2".getBytes(), 1, 1, 1, digestType, customMetadata },
//                { 3, "test3".getBytes(), 1, 2, 2, digestType, customMetadata, new BKException.BKIncorrectParameterException() },
//                { 3, "test3".getBytes(), 2, 1, 2, digestType, customMetadata, new BKException.BKIncorrectParameterException() }

        });
    }

    public LedgerHandleTest(int numBookies, byte[] entry, int ensembleSize, int writeQuorum, int ackQuorum, DigestType digestType, Map<String, byte[]> customMetadata, BKException exception) {
        super(numBookies, 10);
        this.entry = entry;
        this.ensembleSize = ensembleSize;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
        this.digestType = digestType;
        this.customeMetadata = customMetadata;
        this.exception = exception;
    }

//    class LedgerCreationCallback implements AsyncCallback.CreateCallback {
//        private CompletableFuture<LedgerHandle> future;
//
//        public LedgerCreationCallback(CompletableFuture<LedgerHandle> future) {
//            this.future = future;
//        }
//
//        public void createComplete(int returnCode, LedgerHandle handle, Object ctx) {
//            System.out.println(returnCode);
//            System.out.println("Ledger successfully created");
//            if (returnCode != BKException.Code.OK) {
//                future.completeExceptionally(BKException.create(returnCode).fillInStackTrace());
//            } else {
//                future.complete(handle);
//            }
//        }
//    }
//
//    @Test
//    public void testCreate() {
//        try {
//            CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
//            bkc.asyncCreateLedger(ensembleSize, writeQuorum, ackQuorum, digestType, testPasswd, new LedgerCreationCallback(future), null, Collections.emptyMap());
//            LedgerHandle ledgerHandle = future.get();
//            assertTrue(true);
//        } catch (InterruptedException e) {
//            fail(e.getMessage());
//        } catch (ExecutionException e) {
//            fail(e.getMessage());
//        }
//    }

//    @Test
//    public void testCreate() {
//        try {
//            LedgerHandle ledgerHandle = bkc.createLedger(ensembleSize, writeQuorum, ackQuorum, digestType, testPasswd);
//            assertTrue(true);
//        } catch (InterruptedException e) {
//            fail(e.getMessage());
//        } catch (BKException e) {
//            e.printStackTrace();
//        }
//    }

    @Test
    public void testCreate() {
        try {
            WriteHandle wh = bkc.newCreateLedgerOp()
                    .withPassword(testPasswd)
                    .withEnsembleSize(ensembleSize)
                    .withWriteQuorumSize(writeQuorum)
                    .withAckQuorumSize(ackQuorum)
                    .execute()          // execute the creation op
                    .get();
            assertEquals(wh.getLedgerMetadata().getState().name(), "OPEN");
            assertFalse(wh.isClosed());
        } catch (InterruptedException e) {
            fail();
        } catch (ExecutionException e) {
            if (exception == null ){
                fail();
            }
            if (e.getCause() instanceof BKException) {
                BKException cause = (BKException) e.getCause();
                assertEquals(cause.getCode(), exception.getCode());
            } else {
                fail();
            }
        }
    }

    @Test
    public void testCustomMetadata() {
        try {
            LedgerHandle ledgerHandle = bkc.createLedger(ensembleSize, writeQuorum, ackQuorum, digestType, testPasswd, customeMetadata);
            Map<String, byte[]> customMetadata = new HashMap<>();
            customMetadata.put("m1-key", "m1-value".getBytes());
            customMetadata.put("m2-key", "m2-value".getBytes());
            assertEquals(customMetadata.values().size(), ledgerHandle.getCustomMetadata().values().size());
            // Displaying the Map1
            System.out.println("First Map: "
                    + customMetadata);

            // Displaying the Map2
            System.out.println("Second Map: "
                    + ledgerHandle.getCustomMetadata());

            // Checking the equality
            System.out.println("Equality: " + customMetadata.equals(ledgerHandle.getCustomMetadata()));

            assertEquals(customMetadata.keySet(), ledgerHandle.getCustomMetadata().keySet());
            customMetadata.keySet().forEach(k -> {
                assertArrayEquals(customMetadata.get(k), ledgerHandle.getCustomMetadata().get(k));
            });
        } catch (Exception e) {
//            e.printStackTrace();
        }
    }

    @Test
    public void testAddEntry() {
        try {
            LedgerHandle ledgerHandle = bkc.createLedger(ensembleSize, writeQuorum, ackQuorum, digestType, testPasswd);
            System.out.println(new String(ledgerHandle.getLedgerKey()));
            long entryId = ledgerHandle.addEntry(entry);
            assertEquals(entryId, ledgerHandle.readLastEntry().getEntryId());
            assertArrayEquals(entry, ledgerHandle.readLastEntry().getEntry());
        } catch (Exception e) {
        }
    }


}
