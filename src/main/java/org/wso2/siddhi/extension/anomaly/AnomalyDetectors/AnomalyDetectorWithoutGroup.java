/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.anomaly.AnomalyDetectors;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class AnomalyDetectorWithoutGroup extends AnomalyDetector {
    private long timePeriod;
    private int threshold;
    private int step;
    private Map<ExpressionExecutor,String> filters;
    private Map<String,Object[]> events = new HashMap<String, Object[]>();
    private boolean summarize;

    @Override
    public void init(boolean summarize, Map<ExpressionExecutor, String> filters, long timePeriod, int threshold, int step) {
        this.summarize = summarize;
        this.filters = filters;
        this. threshold = threshold;
        this. step = step;
        this.timePeriod = timePeriod;
    }

    @Override
    public Object[] process(String privateKey, String groupBy, long currentTime, String summary, ComplexEvent event) {
        removeEvents(currentTime);
        boolean validEvent = checkEvent(event, this.filters);
        if (validEvent) {
            events.put(privateKey, new Object[]{currentTime, summary});
        } else {
            events.remove(privateKey);
        }

        if (summarize) {
            return detectAnomalyWithSummary(currentTime);
        } else {
            return detectAnomalyWihtoutSummary(currentTime);
        }
    }

    @Override
    public void restore(Object[] status) {
        this.timePeriod = (Long) status[0];
        this.threshold = (Integer) status[1];
        this.step = (Integer) status[2];
        this.filters  = (Map<ExpressionExecutor,String>) status[3];
        this.events = (Map<String, Object[]>) status[4];
        this.summarize = (Boolean) status[5];
    }

    @Override
    public Object[] getStatus() {
        return new Object[]{
                this.timePeriod,
                this.threshold,
                this.step,
                this.filters,
                this.events,
                this.summarize
        };
    }

    private Object[] detectAnomalyWithSummary(long currentTime) {
        long minTime = currentTime;
        String summaryTable = "<tbody>";
        if (events.size() == threshold || (events.size() - threshold) % step == 0) {
            for (Map.Entry<String, Object[]> subEvent : events.entrySet()) {
                Object[] val = subEvent.getValue();
                if (minTime > (Long) val[0]) {
                    minTime = (Long) val[0];
                }

                summaryTable += (String) val[1];
            }
            summaryTable += "</tbody>";
            return new Object[]{events.size(), minTime, currentTime, summaryTable};
        }
        return null;
    }

    private Object[] detectAnomalyWihtoutSummary(long currentTime) {
        long minTime = currentTime;
        if (events.size() == threshold || (events.size() - threshold) % step == 0) {
            for (Map.Entry<String, Object[]> subEvent : events.entrySet()) {
                Object[] val = subEvent.getValue();
                if (minTime > (Long) val[0]) {
                    minTime = (Long) val[0];
                }
            }
            return new Object[]{events.size(), minTime, currentTime};
        }
        return null;
    }

    private void removeEvents(long currentTime) {
        for(Iterator<Map.Entry<String, Object[]>> it = events.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, Object[]> event = it.next();
            if ((Long) event.getValue()[0] + timePeriod <= currentTime) {
                    it.remove();
            }
        }
    }

}
