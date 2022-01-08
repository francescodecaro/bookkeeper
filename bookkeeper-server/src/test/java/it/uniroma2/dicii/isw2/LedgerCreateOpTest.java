package it.uniroma2.dicii.isw2;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LedgerCreateOpTest extends BookKeeperClusterTestCase {

    private static final String OPEN_STATE = "OPEN";
    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private Map<String, byte[]> customMetadata;
    private int exceptionCode;

    @Parameterized.Parameters
    public static Collection params() {
        return Arrays.asList(new Object[][]{
                { 3, 2, 1, Collections.emptyMap(), BKException.Code.OK },
                { 1, 1, 1, Collections.emptyMap(), BKException.Code.OK },
                { 1, 2, 2, Collections.emptyMap(), BKException.Code.IncorrectParameterException },
                { 2, 1, 2, Collections.emptyMap(), BKException.Code.IncorrectParameterException },
                { 4, 2, 1, Collections.emptyMap(), BKException.Code.NotEnoughBookiesException },
                { 1, 1, 1, null, BKException.Code.IncorrectParameterException }
        });
    }

    public LedgerCreateOpTest(int ensSize, int writeQuorumSize, int ackQuorumSize, Map<String, byte[]> customMetadata, int exceptionCode) {
        super(3);
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.customMetadata = customMetadata;
        this.exceptionCode = exceptionCode;
    }

    @Test
    public void createTest() {
        try {
            WriteHandle wh = bkc.newCreateLedgerOp()
                    .withEnsembleSize(ensSize)
                    .withPassword("password".getBytes())
                    .withWriteQuorumSize(writeQuorumSize)
                    .withAckQuorumSize(ackQuorumSize)
                    .withCustomMetadata(customMetadata)
                    .execute()          // execute the creation op
                    .get();

            assertEquals(customMetadata.keySet(), wh.getLedgerMetadata().getCustomMetadata().keySet());
            customMetadata.keySet().forEach(k -> {
                assertArrayEquals(customMetadata.get(k), wh.getLedgerMetadata().getCustomMetadata().get(k));
            });

            assertEquals(wh.getLedgerMetadata().getState().name(), OPEN_STATE);
            assertFalse(wh.isClosed());

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
}