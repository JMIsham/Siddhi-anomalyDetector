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

package org.wso2.siddhi.extension.anomaly;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.extension.anomaly.AnomalyDetectors.AnomalyDetector;
import org.wso2.siddhi.extension.anomaly.AnomalyDetectors.AnomalyDetectorWithGroup;
import org.wso2.siddhi.extension.anomaly.AnomalyDetectors.AnomalyDetectorWithoutGroup;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Sample Query1 (time window, length window, nextX, y, x):
 * from InputStream#timeseries:detectAnomaly(3000, uniqueAttribute, groupingAttribute, 5, 3, 1,
 *                                           filterAttribute1, "filterValue", true)
 * select *
 * insert into OutputStream;
 */

public class EventAnomalyDetector extends StreamProcessor {
    private long timePeriod;
    private boolean primaryKey = true;
    private boolean groupBy = true;
    private int threshold;
    private int step;
    private int numFilters ;
    private Map<ExpressionExecutor,String> filters = new HashMap<ExpressionExecutor, String>();
    private boolean summarize = true;
    private AnomalyDetector anomalyDetector;

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        //set Time Period
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT) {
                this.timePeriod = (Integer) ((ConstantExpressionExecutor)
                        attributeExpressionExecutors[0]).getValue();
            } else {
                throw new ExecutionPlanCreationException("Time has to be a long value");
            }
        } else {
            throw new ExecutionPlanCreationException("Time has to be a constant");
        }

        //check if there is a primary Key
        //for now there is no primaryKey-less implementation
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.BOOL &&
                    !(Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()
                    ) {
                this.primaryKey = false;
            } else {
                throw new ExecutionPlanCreationException("If there is no any primary key the value has to be " +
                        "boolean false");
            }
        }

        //check if there us a grouping attribute
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.BOOL &&
                   ! (Boolean) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()
                    ) {
                this.groupBy = false;
            } else {
                throw new ExecutionPlanCreationException("If there is no grouping the value has to be " +
                        "boolean false");
            }
        }


        //Threshold
        if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor &&
                attributeExpressionExecutors[3].getReturnType() == Attribute.Type.INT
                ) {
            this.threshold = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();
        } else {
                throw new ExecutionPlanCreationException("The Event threshold must be an integer");
        }

        //Once the Threshold is met what is the step for the next Threshold
        //when there is a primary key the existing data will be replaced if a new event arrives with the same primaryKey
        if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor
                && attributeExpressionExecutors[4].getReturnType() == Attribute.Type.INT
                && (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue() >0
                ) {
            this.step = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue();
        } else {
            throw new ExecutionPlanCreationException(("The incrementing step should be grater than 0"));
        }

        //Number if filters has to consider
        if (attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor &&
                (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[5]).getValue() >=0 ) {
            this.numFilters = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[5]).getValue();
        } else {
            throw new ExecutionPlanCreationException("Number of filters has to be an integer");
        }

        //Storing the filter conditions
        System.out.print(numFilters);
        for (int itr = 0; itr < numFilters; ++itr) {
            if (attributeExpressionExecutors[6 + itr * 2] instanceof ConstantExpressionExecutor ) {
                throw new ExecutionPlanCreationException("Constant values have been given for attribute");
            } else {
                if (attributeExpressionExecutors[7 + itr * 2] instanceof ConstantExpressionExecutor) {
                    filters.put(attributeExpressionExecutors[ 6 + itr * 2] , (String) (
                            (ConstantExpressionExecutor) attributeExpressionExecutors[7 + itr * 2]).getValue());
                } else {
                    throw new ExecutionPlanCreationException("The filter condition should be a constant");
                }
            }
        }

        //Check if there is an event summary is needed for each event
        ExpressionExecutor summary = attributeExpressionExecutors[6 + numFilters * 2];
        if (summary instanceof ConstantExpressionExecutor) {
            if (summary.getReturnType() == Attribute.Type.BOOL
                    && !(Boolean) ((ConstantExpressionExecutor) summary).getValue()
                    ) {
                summarize = false;
            } else {
                throw new ExecutionPlanCreationException("The summary should be false if no summary needed");
            }
        }

        //outputs
        ArrayList<Attribute> attributes;


        if (groupBy) {
            anomalyDetector = new AnomalyDetectorWithGroup();
        } else {
            anomalyDetector = new AnomalyDetectorWithoutGroup();
        }
        anomalyDetector.init(summarize, filters, timePeriod, threshold, step);

        if (summarize) {
            //the summary will be aggregated and enclosed with <tbody></tbody>
            //preferred to have individual summary has the following formate
            // <tr>
            //  <td> data1 </td>
            //  <td> data2 </td>
            //  ..
            // </tr>
            attributes = new ArrayList<Attribute>(4);
            attributes.add(new Attribute("EventCount",Attribute.Type.LONG));
            attributes.add(new Attribute("StartingTime",Attribute.Type.LONG));
            attributes.add(new Attribute("EndingTime",Attribute.Type.LONG));
            attributes.add(new Attribute("HTMLSummary",Attribute.Type.STRING));

        } else {
            attributes = new ArrayList<Attribute>(3);
            attributes.add(new Attribute("EventCount",Attribute.Type.LONG));
            attributes.add(new Attribute("StartingTime",Attribute.Type.LONG));
            attributes.add(new Attribute("EndingTime",Attribute.Type.LONG));
        }
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                ComplexEvent complexEvent = streamEventChunk.next();
                String primaryKey = (String) attributeExpressionExecutors[1].execute(complexEvent);
                String grouping = null;
                if (groupBy) {
                    grouping = (String) attributeExpressionExecutors[2].execute(complexEvent);
                }
                long currentTime = executionPlanContext.getTimestampGenerator().currentTime();

                String summary = null;
                if (summarize) {
                    summary = (String) attributeExpressionExecutors[6 + numFilters * 2].execute(complexEvent);
                }

                Object[] outputData = anomalyDetector.process(primaryKey, grouping, currentTime, summary, complexEvent);
                //if no anomaly is been detected the output will be null
                if (outputData == null) {
                    streamEventChunk.remove();
                } else {
                    complexEventPopulater.populateComplexEvent(complexEvent, outputData);
                }
            }
        }
        nextProcessor.process(streamEventChunk);

    }

    @Override
    public void start() {
        //nothing to start
    }

    @Override
    public void stop() {
        //nothing to stop
    }

    @Override
    public Object[] currentState() {
        return new Object[]{
                anomalyDetector.getStatus(),
                this.primaryKey,
                this.groupBy
        };
    }

    @Override
    public void restoreState(Object[] state) {
        this.primaryKey = (Boolean) state[1];
        this.groupBy = (Boolean) state[2];
        if (groupBy) {
            this.anomalyDetector = new AnomalyDetectorWithGroup();
        } else {
            this.anomalyDetector = new AnomalyDetectorWithoutGroup();
        }
        Object[] status = (Object[]) state[0];
        anomalyDetector.restore(status);
        this.timePeriod = (Long) status[0];
        this.threshold = (Integer) status[1];
        this.step = (Integer) status[2];
        this.filters  = (Map<ExpressionExecutor,String>) status[3];
        this.summarize = (Boolean) status[5];
    }


}
