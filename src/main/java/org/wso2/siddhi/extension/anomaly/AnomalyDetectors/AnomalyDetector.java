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

import java.util.Map;


public abstract class AnomalyDetector {
    public abstract void init(boolean summarize, Map<ExpressionExecutor, String> filters, long timePeriod,
                              int threshold, int step);
    public abstract Object[] process(String privateKey, String groupBy, long currentTime, String summary,
                                     ComplexEvent event);

    public abstract void restore(Object[] status);

    public abstract  Object[] getStatus();

    boolean checkEvent(ComplexEvent event, Map<ExpressionExecutor, String> filters) {
        for (Map.Entry<ExpressionExecutor, String> entry : filters.entrySet()) {
            ExpressionExecutor attribute = entry.getKey();
            String val = entry.getValue();
            if ( !attribute.execute(event).equals(val)) {
                return false;
            }
        }
        return true;
    }
}
