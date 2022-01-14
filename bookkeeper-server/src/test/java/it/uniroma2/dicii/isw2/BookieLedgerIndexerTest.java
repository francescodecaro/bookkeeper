package it.uniroma2.dicii.isw2;


public class BookieLedgerIndexerTest  {

//    BookieLedgerIndexer bookieLedgerIndexer;
//    LedgerManager ledgerManager;
//    long ledgerId;
//
//    public BookieLedgerIndexerTest() {
//        super(3);
//    }
//
//    @Before
//    public void configure() throws BKException, InterruptedException {
//        bookieLedgerIndexer = new BookieLedgerIndexer(bkc.getLedgerManager());
//        ledgerId = bkc.createLedger(3, 2, 1, BookKeeper.DigestType.MAC, "".getBytes()).getId();
//    }
//
//
//    @Test
//    public void testBookieToLedger() throws BKException, InterruptedException, ReplicationException.BKAuditException {
//        ConcurrentHashMap<String, Set<Long>> indexer = new ConcurrentHashMap<String, Set<Long>>();
//        bkc.getBookieInfo().forEach((k,v) -> {
//            Set<Long> ledgers = new HashSet<Long>();
//            ledgers.add(ledgerId);
//            indexer.put(k.getId(), ledgers);
//        });
//
//        Map<String, Set<Long>> bookieIndexer = bookieLedgerIndexer.getBookieToLedgerIndex();
//        assertEquals(indexer.keySet(), bookieIndexer.keySet());
//        indexer.keySet().forEach(k -> {
//            assertEquals(indexer.get(k), bookieIndexer.get(k));
//        });
//    }

}
