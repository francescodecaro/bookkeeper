package it.uniroma2.dicii.isw2;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.*;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LedgerCreateOpTest extends BookKeeperClusterTestCase {

    private static final String OPEN_STATE = "OPEN";
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private DigestType digestType;
    private String password;
    private Map<String, byte[]> customMetadata;
    private long ledgerId;
    private int exceptionCode;
    private int exceptionCodeAdv;
    private boolean opportunisticStriping;
    private boolean systemTime;

    @Parameterized.Parameters
    public static Collection params() {
        Map<String, byte[]> customMetadata = new HashMap<>();
        customMetadata.put("key", "value".getBytes());
        return Arrays.asList(new Object[][]{
                {0, 1, 2, null, null, Collections.emptyMap(), 0, BKException.Code.IncorrectParameterException, BKException.Code.IncorrectParameterException, false, true },
                {0, 1, 2, null, null, Collections.emptyMap(), 0, BKException.Code.IncorrectParameterException, BKException.Code.IncorrectParameterException, true, false },

                {1, 1, 1, DigestType.CRC32, "password", Collections.emptyMap(),  0, BKException.Code.OK, BKException.Code.OK, false, true },
                {1, 1, 1, DigestType.CRC32, "", Collections.emptyMap(), 0, BKException.Code.OK, BKException.Code.OK, true, false },

                {2, 2, 1, DigestType.CRC32, "", Collections.emptyMap(), 0, BKException.Code.OK, BKException.Code.OK, false, false },
                {2, 2, 1, DigestType.CRC32, "", Collections.emptyMap(), 0, BKException.Code.OK, BKException.Code.OK, true, true },


                { 3, 2, 1, DigestType.CRC32C, "password", Collections.emptyMap(), 0, BKException.Code.OK, BKException.Code.OK, true, false },
                { 3, 2, 1, DigestType.MAC, "password", Collections.emptyMap(), 0, BKException.Code.OK, BKException.Code.OK, false, false },
                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, true, true },



                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, -1L, BKException.Code.OK, BKException.Code.IncorrectParameterException, true, false },
                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, -1L, BKException.Code.OK, BKException.Code.IncorrectParameterException, false, true },

                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, -2L, BKException.Code.OK, BKException.Code.IncorrectParameterException, true, false },
                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, -2L, BKException.Code.OK, BKException.Code.IncorrectParameterException, false, true },


                // Long.MIN_VALUE to pass null
                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, Long.MIN_VALUE, BKException.Code.OK, BKException.Code.OK, true, false },
                { 3, 2, 1, DigestType.DUMMY, "password", customMetadata, Long.MIN_VALUE, BKException.Code.OK, BKException.Code.OK, false, true },

                { 3, 3, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, false, true },
                { 3, 3, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, true, false },


                { 4, 1, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.NotEnoughBookiesException, BKException.Code.NotEnoughBookiesException, false, true },
                { 4, 1, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, true, false },

                { 4, 2, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.NotEnoughBookiesException, BKException.Code.NotEnoughBookiesException, false, true },
                { 4, 2, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, true, false },

                { 4, 3, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.NotEnoughBookiesException, BKException.Code.NotEnoughBookiesException, false, true },
                { 4, 3, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, true, false },

                { 5, 3, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.NotEnoughBookiesException, BKException.Code.NotEnoughBookiesException, false, true },
                { 5, 3, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.OK, BKException.Code.OK, true, false },

                { 4, 4, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.NotEnoughBookiesException, BKException.Code.NotEnoughBookiesException, false, true },
                { 4, 4, 1, DigestType.DUMMY, "password", customMetadata, 0, BKException.Code.NotEnoughBookiesException, BKException.Code.NotEnoughBookiesException, true, false },

        });
    }

    public LedgerCreateOpTest(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType,
                              String password, Map<String, byte[]> customMetadata,
                              long ledgerId, int exceptionCode, int exceptionCodeAdv, boolean opportunisticStriping, boolean systemTime) {
        super(3);
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.password = password;
        this.customMetadata = customMetadata;
        this.ledgerId = ledgerId;
        this.exceptionCode = exceptionCode;
        this.exceptionCodeAdv = exceptionCodeAdv;
        this.opportunisticStriping = opportunisticStriping;
        this.systemTime = systemTime;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        baseClientConf.setOpportunisticStriping(opportunisticStriping);
        baseClientConf.setStoreSystemtimeAsLedgerCreationTime(systemTime);
        setUp("/ledgers");
    }

    @Test
    public void createTest() {
        try {
            WriteHandle wh = bkc.newCreateLedgerOp()
                    .withEnsembleSize(ensSize)
                    .withDigestType(digestType)
                    .withPassword(password.getBytes())
                    .withWriteQuorumSize(writeQuorumSize)
                    .withAckQuorumSize(ackQuorumSize)
                    .withCustomMetadata(customMetadata)
                    .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                    .execute()          // execute the creation op
                    .get();

            if (exceptionCode != BKException.Code.OK) {
                fail();
            }

            assertEquals(customMetadata.keySet(), wh.getLedgerMetadata().getCustomMetadata().keySet());
            customMetadata.keySet().forEach(k -> {
                assertArrayEquals(customMetadata.get(k), wh.getLedgerMetadata().getCustomMetadata().get(k));
            });

            assertEquals(wh.getLedgerMetadata().getState().name(), OPEN_STATE);
            assertFalse(wh.isClosed());

        } catch (NullPointerException e){
            assertThat(Arrays.asList(digestType, password), hasItem(nullValue()));
        } catch (InterruptedException e) {
            fail("Test Timeout");
        } catch (ExecutionException e) {
            if (exceptionCode == BKException.Code.OK || !(e.getCause() instanceof BKException)){
                fail();
            }
            BKException cause = (BKException) e.getCause();
            assertEquals(exceptionCode, cause.getCode());
        }
    }

    @Test
    public void createAdvTest() {
        try {
            CreateAdvBuilder createAdvBuilder = bkc.newCreateLedgerOp()
                    .withEnsembleSize(ensSize)
                    .withDigestType(digestType)
                    .withPassword(password.getBytes())
                    .withWriteQuorumSize(writeQuorumSize)
                    .withAckQuorumSize(ackQuorumSize)
                    .withCustomMetadata(customMetadata)
                    .withWriteFlags(WriteFlag.DEFERRED_SYNC)
                    .makeAdv();

            if (ledgerId != Long.MIN_VALUE) {
                createAdvBuilder = createAdvBuilder.withLedgerId(ledgerId);
            }

            WriteAdvHandle wh = createAdvBuilder
                    .execute()          // execute the creation op
                    .get();

            if (exceptionCodeAdv != BKException.Code.OK) {
                fail();
            }

            assertEquals(customMetadata.keySet(), wh.getLedgerMetadata().getCustomMetadata().keySet());
            customMetadata.keySet().forEach(k -> {
                assertArrayEquals(customMetadata.get(k), wh.getLedgerMetadata().getCustomMetadata().get(k));
            });

            assertEquals(wh.getLedgerMetadata().getState().name(), OPEN_STATE);
            assertFalse(wh.isClosed());

        } catch (NullPointerException e){
            assertThat(Arrays.asList(digestType, password), hasItem(nullValue()));
        } catch (InterruptedException e) {
            fail("Test Timeout");
        } catch (ExecutionException e) {
            if (exceptionCodeAdv == BKException.Code.OK || !(e.getCause() instanceof BKException)){
                fail();
            }
            BKException cause = (BKException) e.getCause();
            assertEquals(exceptionCodeAdv, cause.getCode());
        }
    }
}