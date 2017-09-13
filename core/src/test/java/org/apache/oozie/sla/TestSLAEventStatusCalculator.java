/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.sla;

import org.apache.oozie.client.event.SLAEvent;
import org.junit.Before;
import org.junit.Test;
import parquet.Strings;

import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSLAEventStatusCalculator {
    private SLASummaryBean bean;

    @Before
    public void setUp() {
        bean = new SLASummaryBean();
    }

    @Test
    public void whenNoAttributesAreSetEventStatusIsEmpty() {
        final String eventStatus = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();

        assertTrue(Strings.isNullOrEmpty(eventStatus));
    }

    @Test
    public void whenStartFieldsAreSetEventStatusStartIsFilled() {
        bean.setExpectedStart(minuteDiff(-2));

        final String eventStatusNotStartedAndOverdue = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.START_MISS.toString(), eventStatusNotStartedAndOverdue);

        bean.setExpectedStart(now());

        final String eventStatusNotStarted = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertTrue(Strings.isNullOrEmpty(eventStatusNotStarted));

        bean.setActualStart(minuteDiff(2));

        final String eventStatusStartedLate = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.START_MISS.toString(), eventStatusStartedLate);

        bean.setActualStart(minuteDiff(-2));

        final String eventStatusStartedEarly = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.START_MET.toString(), eventStatusStartedEarly);
    }

    @Test
    public void whenDurationFieldsAreSetEventStatusDurationIsFilled() {
        bean.setExpectedDuration(-1);

        final String eventStatusDurationUnset = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertTrue(Strings.isNullOrEmpty(eventStatusDurationUnset));

        bean.setExpectedDuration(60_000);
        bean.setActualDuration(60_001);

        final String eventStatusDurationOverdue = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.DURATION_MISS.toString(), eventStatusDurationOverdue);

        bean.setExpectedDuration(60_000);
        bean.setActualDuration(60_000);

        final String eventStatusDurationMet = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.DURATION_MET.toString(), eventStatusDurationMet);

        bean.setExpectedDuration(60_000);
        bean.setActualDuration(-1);
        bean.setActualStart(null);

        final String eventStatusActualDurationAndActualStartUnset =
                new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertTrue(Strings.isNullOrEmpty(eventStatusActualDurationAndActualStartUnset));

        bean.setExpectedDuration(60_000);
        bean.setActualDuration(-1);
        bean.setActualStart(minuteDiff(-1));

        final String eventStatusCalculatedDurationMet = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertTrue(Strings.isNullOrEmpty(eventStatusCalculatedDurationMet));

        bean.setExpectedDuration(60_000);
        bean.setActualDuration(-1);
        bean.setActualStart(minuteDiff(-2));

        final String eventStatusCalculatedDurationMetSecondTime =
                new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.DURATION_MISS.toString(), eventStatusCalculatedDurationMetSecondTime);
    }

    @Test
    public void whenEndFieldsAreSetEventStatusEndIsFilled() {
        bean.setExpectedEnd(minuteDiff(-2));

        final String eventStatusNotFinishedAndOverdue = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.END_MISS.toString(), eventStatusNotFinishedAndOverdue);

        bean.setExpectedEnd(now());

        final String eventStatusNotFinished = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertTrue(Strings.isNullOrEmpty(eventStatusNotFinished));

        bean.setActualEnd(minuteDiff(2));

        final String eventStatusFinishedLate = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.END_MISS.toString(), eventStatusFinishedLate);

        bean.setActualEnd(minuteDiff(-2));

        final String eventStatusFinishedEarly = new SLASummaryBean.SLAEventStatusCalculator(bean).calculate().toString();
        assertEquals(SLAEvent.EventStatus.END_MET.toString(), eventStatusFinishedEarly);
    }

    private Date now() {
        final Calendar now = Calendar.getInstance();

        return now.getTime();
    }

    private Date minuteDiff(final int amount) {
        final Calendar oneMinuteLater = (Calendar) Calendar.getInstance().clone();
        oneMinuteLater.add(Calendar.MINUTE, amount);

        return oneMinuteLater.getTime();
    }
}