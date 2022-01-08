package it.uniroma2.dicii.isw2;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
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
//    private Map<String, byte[]> customMetadata;
    private DigestType digestType;
    private String password;
    private int exceptionCode;

    @Parameterized.Parameters
    public static Collection params() {
        return Arrays.asList(new Object[][]{
                {0, 1, 2, null, null, BKException.Code.IncorrectParameterException },
                {1, 1, 1, DigestType.CRC32, "password", BKException.Code.OK }
//                { 3, 2, 1, Collections.emptyMap(), BKException.Code.OK },
//                { 1, 1, 1, Collections.emptyMap(), BKException.Code.OK },
//                { 1, 2, 2, Collections.emptyMap(), BKException.Code.IncorrectParameterException },
//                { 2, 1, 2, Collections.emptyMap(), BKException.Code.IncorrectParameterException },
//                { 4, 2, 1, Collections.emptyMap(), BKException.Code.NotEnoughBookiesException },
//                { 1, 1, 1, null, BKException.Code.IncorrectParameterException }
        });
    }

    public LedgerCreateOpTest(int ensSize, int writeQuorumSize, int ackQuorumSize, DigestType digestType, String password, int exceptionCode) {
        super(3);
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.password = password;
        this.exceptionCode = exceptionCode;
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
                    .execute()          // execute the creation op
                    .get();

//            assertEquals(customMetadata.keySet(), wh.getLedgerMetadata().getCustomMetadata().keySet());
//            customMetadata.keySet().forEach(k -> {
//                assertArrayEquals(customMetadata.get(k), wh.getLedgerMetadata().getCustomMetadata().get(k));
//            });

            assertEquals(wh.getLedgerMetadata().getState().name(), OPEN_STATE);
            assertFalse(wh.isClosed());

        } catch (NullPointerException e){
            assertThat(Arrays.asList(digestType, password), hasItem(nullValue()));
//            assertNull(digestType);
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