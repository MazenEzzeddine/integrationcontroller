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
            scaleLogicTail();



            log.info("Sleeping for 1 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(1000);
        }
    }



//    private static void scaleLogic() throws InterruptedException {
//
//        if  (Duration.between(bp.LastUpScaleDecision, Instant.now()).getSeconds() >10) {
//            bp.scaleAsPerBinPack();
//
//        } else {
//            log.info("No scale group 1 cooldown");
//        }
//
//
//    }



    private static void scaleLogicTail() throws InterruptedException {
        if  (Duration.between(BinPackLag2.LastUpScaleDecision, Instant.now()).getSeconds() >3) {
            BinPackState2.scaleAsPerBinPack();
            if (BinPackState2.action.equals("up") || BinPackState2.action.equals("down") || BinPackState2.action.equals("REASS") ) {
                BinPackLag2.scaleAsPerBinPack();
            }
        } else {
            log.info("No scale group 1 cooldown");
        }
    }





  /*  private static void scaleLogicReb() throws InterruptedException {

        if (Duration.between(bp.LastUpScaleDecision, Instant.now()).getSeconds() > 10) {
            boolean reply = bp.scaleAsPerBinPackPrologue();

            if (reply) {
                bp.scaleAsPerBinPack();
            }
        } else {
            log.info("No scale group 1 cooldown");
        }


    }
*/

}
