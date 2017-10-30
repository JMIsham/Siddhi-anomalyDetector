package org.wso2.siddhi.extension.anomaly;


import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
public class EventAnomalyDetectorTestCase {
        static final Logger log = Logger.getLogger(org.wso2.siddhi.extension.anomaly.EventAnomalyDetector.class);
        private volatile int count;
        private volatile boolean eventArrived;

        @Before
        public void init() {
            count = 0;
            eventArrived = false;
        }

        @Test
        public void testEventAnomalyDetection() throws InterruptedException {
            log.info("General TestCase");
            SiddhiManager siddhiManager = new SiddhiManager();

            String inStreamDefinition = "define stream inputStream (issueID String, projectID String, " +
                    "issueStatus String, otherField String);";
            String query = ("@info(name = 'query1') from inputStream#anomaly:detectAnomaly(10000, issueID, " +
                    "false, 5,3, 1, issueStatus, 'WOW', issueID) " +
                    "select * " +
                    "insert into outputStream;");
            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition +
                    query);

            executionPlanRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventArrived = true;
                    for (Event event : inEvents) {
                        count++;
                    }
                }
            });

            InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
            executionPlanRuntime.start();

            inputHandler.send(new Object[]{"001", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"002", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"003", "457", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"004", "457", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"005", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"006", "456", "WO", "other"});
            inputHandler.send(new Object[]{"007", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"008", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"009", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"001", "456", "WO", "other"});
            inputHandler.send(new Object[]{"010", "456", "WOW", "other"});
            Thread.sleep(500);
            inputHandler.send(new Object[]{"011", "456", "WO", "other"});
            Thread.sleep(1000);
            Assert.assertEquals(3, count);
            Assert.assertTrue(eventArrived);
            executionPlanRuntime.shutdown();

        }

    }
