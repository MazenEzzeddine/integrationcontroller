import org.apache.kafka.common.ConsumerGroupState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

public class Controller implements Runnable {
    private static final Logger log = LogManager.getLogger(Controller.class);


    static long  decisionIntervalms = 1000;





    private static void initialize() throws InterruptedException, ExecutionException {

        Constants.init();
        Lag.readEnvAndCrateAdminClient();
  /*      log.info("Warming 15  seconds.");
        Thread.sleep(15 * 1000);*/

        log.info("Warming  4  minutes");
        Thread.sleep(60*4 * 1000);
        while (true) {
            log.info("Querying Prometheus");
          //  ArrivalProducer.callForArrivals();
            ArrivalRates.arrivalRateTopic1();
            Lag.getCommittedLatestOffsetsAndLag();
            log.info("--------------------");
            log.info("--------------------");


            if(ArrivalRates.processingRate != 0) {
                scaleLogicTail3();
            }

            log.info("Sleeping for 1 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(decisionIntervalms);
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


/*
    private static void scaleLogicTail2() throws InterruptedException, ExecutionException {
        if (Lag.queryConsumerGroup() != BinPackState2.size) {
            log.info("no action, previous action is not seen yet");
            return;
        }
        BinPackState2.scaleAsPerBinPack();
        if (BinPackState2.action.equals("up") || BinPackState2.action.equals("down")
                || BinPackState2.action.equals("REASS")) {
            BinPackLag2.scaleAsPerBinPack();
        }
    }
*/




    private static void scaleLogicTail3() throws InterruptedException, ExecutionException {

        //TODO BinPackLag3.size()=> it is OK
        if (Lag.queryConsumerGroup() != BinPackState3.size  || /*BinPackLag3.waitAssign*/ Lag.queryConsumerGroupState() != ConsumerGroupState.STABLE
              /*  Duration.between(BinPackLag3.LastUpScaleDecision,Instant.now()).getSeconds() < 10*/)  /*&& !BinPackLag3.waitAssign*/ {
            //BinPackLag3.LastUpScaleDecision = Instant.now();

            log.info("no action, previous action is not seen yet");
            return;
        }

        BinPackState3.scaleAsPerBinPack();
        if (BinPackState3.action.equals("up") || BinPackState3.action.equals("down")
                || BinPackState3.action.equals("REASS")) {
            BinPackLag3.scaleAsPerBinPack();
        }
    }


    @Override
    public void run() {
        try {
            initialize();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
