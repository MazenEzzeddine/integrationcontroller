import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);
    // static BinPack4 bp;
     static BinPackState2 bps;
    static BinPackLag2 bpl;


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        initialize();
    }


    private static void initialize() throws InterruptedException, ExecutionException {
        bps = new BinPackState2();
        bpl = new BinPackLag2();
        Lag.readEnvAndCrateAdminClient();
        log.info("Warming 15  seconds.");
        Thread.sleep(15 * 1000);
        while (true) {
            log.info("Querying Prometheus");
            ArrivalProducer.callForArrivals();
            Lag.getCommittedLatestOffsetsAndLag();
            log.info("--------------------");
            log.info("--------------------");



            //scaleLogic();
            scaleLogicTail2();



            log.info("Sleeping for 1 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(1000);
        }
    }







   /* private static void scaleLogicTail() throws InterruptedException {
        if  (Duration.between(BinPackLag2.LastUpScaleDecision, Instant.now()).getSeconds() >3) {
            BinPackState2.scaleAsPerBinPack();
            if (BinPackState2.action.equals("up") || BinPackState2.action.equals("down") || BinPackState2.action.equals("REASS") ) {
                BinPackLag2.scaleAsPerBinPack();
            }
        } else {
            log.info("No scale group 1 cooldown");
        }
    }*/








    private static void scaleLogicTail2() throws InterruptedException, ExecutionException {
        if (Lag.queryConsumerGroup() != BinPackState2.size) {
            log.info("no action, previous action is not seen yet");
            return;
        }
            BinPackState2.scaleAsPerBinPack();
            if (BinPackState2.action.equals("up") || BinPackState2.action.equals("down") || BinPackState2.action.equals("REASS") ) {
                BinPackLag2.scaleAsPerBinPack();
        }
    }


}
