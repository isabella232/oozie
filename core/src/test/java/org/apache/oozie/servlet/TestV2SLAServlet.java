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

package org.apache.oozie.servlet;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.oozie.AppType;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.event.SLAEvent.SLAStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.BatchQueryExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.SLASummaryQueryExecutor;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.util.DateUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class TestV2SLAServlet extends DagServletTestCase {

    private static final boolean IS_SECURITY_ENABLED = false;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testSLAWithoutEventStatus() throws Exception {
        runTest("/v2/sla", V2SLAServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {

                final Date currentTime = new Date(System.currentTimeMillis());
                final Date nominalTime1 = DateUtils.parseDateUTC("2012-06-01T10:00Z");
                final Date nominalTime2 = DateUtils.parseDateUTC("2012-06-02T10:20Z");
                final Date nominalTime3 = DateUtils.parseDateUTC("2012-06-03T14:00Z");
                insertEntriesIntoSLASummaryTable(2, "1-", "-W", "1-C", nominalTime1, "testapp-1", AppType.WORKFLOW_JOB,
                        currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
                insertEntriesIntoSLASummaryTable(3, "2-", "-W", null, nominalTime2, "testapp-2", AppType.WORKFLOW_JOB,
                        currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);
                insertEntriesIntoSLASummaryTable(6, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime, EventStatus.END_MISS, SLAStatus.IN_PROCESS);


                noFilterThrow();

                filterAppNameOnlySuccess(currentTime, nominalTime1);

                filterAppNameAndIdSuccess(currentTime, nominalTime2);

                filterAppNameAndNominalStartSuccess(currentTime, nominalTime3);

                filterAppNameNominalStartAndNominalEndSuccess(currentTime, nominalTime3);

                return null;
            }

            private void noFilterThrow() throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                final URL url = createURL("", queryParams);
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
            }

            private void filterAppNameOnlySuccess(final Date currentTime, final Date nominalTime1) throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                final JSONArray array;
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-1");
                array = getSLAJSONResponse(queryParams);

                // Matches first two - 1-1-W and 1-2-W
                assertSLAJSONResponse(array, 1, 2, "1-", "-W", "1-C", nominalTime1, "testapp-1", AppType.WORKFLOW_JOB,
                        currentTime, Collections.<EventStatus>emptyList());
            }

            private void filterAppNameAndIdSuccess(final Date currentTime, final Date nominalTime2) throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                final JSONArray array;
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-2;id=2-2-W");
                array = getSLAJSONResponse(queryParams);

                // Matches second element - 2-2-W
                assertSLAJSONResponse(array, 2, 2, "2-", "-W", null, nominalTime2, "testapp-2", AppType.WORKFLOW_JOB,
                        currentTime, Collections.<EventStatus>emptyList());
            }

            private void filterAppNameAndNominalStartSuccess(final Date currentTime, final Date nominalTime3)
                    throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                final JSONArray array;
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, "app_name=testapp-3;nominal_start=2012-06-03T16:00Z");
                array = getSLAJSONResponse(queryParams);

                // Matches 3-6 elements - 3-3-W 3-4-W 3-5-W 3-6-W
                assertSLAJSONResponse(array, 3, 6, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime, Collections.<EventStatus>emptyList());
            }

            private void filterAppNameNominalStartAndNominalEndSuccess(final Date currentTime, final Date nominalTime3)
                    throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                final JSONArray array;
                queryParams.put(RestConstants.JOBS_FILTER_PARAM,
                        "parent_id=2-C;nominal_start=2012-06-03T016:00Z;nominal_end=2012-06-03T17:00Z");
                array = getSLAJSONResponse(queryParams);

                // Matches 3rd and 4th element - 3-3-W 3-4-W
                assertSLAJSONResponse(array, 3, 4, "3-", "-W", "2-C", nominalTime3, "testapp-3", AppType.WORKFLOW_JOB,
                        currentTime, Collections.<EventStatus>emptyList());
            }
        });
    }

    public void testSLAWithEventStatus() throws Exception {
        runTest("/v2/sla", V2SLAServlet.class, IS_SECURITY_ENABLED, new Callable<Void>() {
            public Void call() throws Exception {
                final Date currentTime = new Date(System.currentTimeMillis());

                //insert Bundle Job/Action, Coord Job/Action
                final List<JsonBean> beans = new ArrayList<JsonBean> ();
                final CoordinatorJobBean cjBean1 = createCoordJob(CoordinatorJob.Status.SUCCEEDED, false, true);
                beans.add(cjBean1);
                final CoordinatorJobBean cjBean2 = createCoordJob(CoordinatorJob.Status.SUCCEEDED, false, true);
                beans.add(cjBean2);

                BatchQueryExecutor.getInstance().executeBatchInsertUpdateDelete(beans, null, null);

                final Calendar cal = Calendar.getInstance();
                cal.add(Calendar.MINUTE, -12);  //current -12
                final Date actualStartForMet = cal.getTime();
                cal.add(Calendar.MINUTE, 2);   //current -10
                final Date expectedStart = cal.getTime();
                cal.add(Calendar.MINUTE, 1);   //current -9
                final Date actualStartForMiss = cal.getTime();
                cal.add(Calendar.MINUTE, 3);    //current -6
                final Date actualEndForMet = cal.getTime();
                cal.add(Calendar.MINUTE, 1);    //current -5
                final Date expectedEnd = cal.getTime();
                cal.add(Calendar.MINUTE, 2);    //current -3
                final Date actualEndForMiss = cal.getTime();
                cal.add(Calendar.MINUTE, 8);   //current + 5
                final Date futureExpectedEnd = cal.getTime();

                // START_MET, DURATION_MET, END_MET
                insertEntriesIntoSLASummaryTable(cjBean1.getId() + "@1", cjBean1.getId(), "testapp-1",
                        AppType.COORDINATOR_ACTION, currentTime, EventStatus.END_MET, SLAStatus.MET, expectedStart,
                        actualStartForMet, 7, 6, expectedEnd, actualEndForMet, actualStartForMet);

                // START_MISS, DURATION_MISS, END_MISS
                insertEntriesIntoSLASummaryTable(cjBean1.getId() + "@2", cjBean1.getId(), "testapp-1",
                        AppType.COORDINATOR_ACTION, currentTime, EventStatus.END_MISS, SLAStatus.MISS, expectedStart,
                        actualStartForMiss, 5, 6, expectedEnd, actualEndForMiss, actualStartForMet);

                // // START_MISS, DURATION_MISS (still running, Not Ended, but
                // expected Duration/End already passed by now)
                insertEntriesIntoSLASummaryTable(cjBean2.getId() + "@1", cjBean2.getId(), "testapp-1",
                        AppType.COORDINATOR_ACTION, currentTime, EventStatus.DURATION_MISS, SLAStatus.IN_PROCESS, expectedStart,
                        actualStartForMiss, 8, 9, futureExpectedEnd, null, actualStartForMet);

                // START_MISS only, (Not Started YET, and Expected Duration/End
                // Time not yet passed)
                insertEntriesIntoSLASummaryTable(cjBean2.getId() + "@2", cjBean2.getId(), "testapp-1",
                        AppType.COORDINATOR_ACTION, currentTime, null, SLAStatus.NOT_STARTED, expectedStart, null, 10, -1,
                        futureExpectedEnd, null, expectedStart);

                noFilterThrow();

                filterAppNameAndAllEventStatus();

                filterAppNameAndOneEventStatusSuccess(cjBean1, cjBean2);

                filterAppNameAndMultipleEventStatusesSuccess(cjBean1, cjBean2);

                filterAppNameAllAndOneMoreEventStatusThrow();

                return null;
            }

            private void noFilterThrow() throws Exception {
                final Map<String, String> queryParams = new HashMap<>();

                final URL url = createURL("", queryParams);
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
            }

            private void filterAppNameAndAllEventStatus() throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                queryParams.put(RestConstants.TIME_ZONE_PARAM, "GMT");
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, String.format("app_name=%s;event_status=ALL", "testapp-1"));
                final JSONArray array = getSLAJSONResponse(queryParams);

                assertEquals(4, array.size());

                final JSONObject json = (JSONObject) array.get(0);
                final String es = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);

                assertNotNull(es);
            }

            private void filterAppNameAndOneEventStatusSuccess(final CoordinatorJobBean cjBean1, final CoordinatorJobBean cjBean2)
                    throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                queryParams.put(RestConstants.TIME_ZONE_PARAM, "GMT");
                queryParams.put(RestConstants.JOBS_FILTER_PARAM, String.format("app_name=%s;event_status=END_MISS", "testapp-1"));
                final JSONArray array = getSLAJSONResponse(queryParams);

                assertEquals(1, array.size());

                final JSONObject json = (JSONObject) array.get(0);
                final String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);

                assertTrue(parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));

                final String id = (String) json.get(JsonTags.SLA_SUMMARY_ID);

                assertTrue(id.equals(cjBean1.getId() + "@2"));

                final String es = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);

                assertTrue(es.contains(EventStatus.START_MISS.toString()));
                assertTrue(es.contains(EventStatus.DURATION_MISS.toString()));
                assertTrue(es.contains(EventStatus.END_MISS.toString()));
            }

            private void filterAppNameAndMultipleEventStatusesSuccess(final CoordinatorJobBean cjBean1,
                                                                      final CoordinatorJobBean cjBean2)
                    throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                queryParams.put(RestConstants.JOBS_FILTER_PARAM,
                        String.format("app_name=%s;event_status=START_MISS,END_MISS", "testapp-1"));

                final JSONArray array = getSLAJSONResponse(queryParams);
                assertEquals(3, array.size());

                for (int i=0; i < array.size(); i++) {
                    final JSONObject json = (JSONObject) array.get(i);
                    final String id = (String)json.get(JsonTags.SLA_SUMMARY_ID);
                    assertTrue(id.equals(cjBean1.getId()+"@2")
                            || id.equals(cjBean2.getId()+"@1")
                            || id.equals(cjBean2.getId()+"@2"));
                    final String parentId = (String) json.get(JsonTags.SLA_SUMMARY_PARENT_ID);
                    assertTrue(parentId.equals(cjBean1.getId()) || parentId.equals(cjBean2.getId()));
                    final String es = (String) json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS);
                    assertNotNull(es);
                    assertTrue(es.contains(EventStatus.START_MISS.toString()));
                }
            }

            private void filterAppNameAllAndOneMoreEventStatusThrow() throws Exception {
                final Map<String, String> queryParams = new HashMap<>();
                queryParams.put(RestConstants.JOBS_FILTER_PARAM,
                        String.format("app_name=%s;event_status=ALL,START_MISS", "testapp-1"));

                final URL url = createURL("", queryParams);
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");

                assertEquals(HttpServletResponse.SC_BAD_REQUEST, conn.getResponseCode());
            }
        });
    }

    private JSONArray getSLAJSONResponse(final Map<String, String> queryParams) throws Exception {
        final URL url = createURL("", queryParams);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        assertEquals(HttpServletResponse.SC_OK, conn.getResponseCode());
        assertTrue(conn.getHeaderField("content-type").startsWith(RestConstants.JSON_CONTENT_TYPE));
        final JSONObject json = (JSONObject) JSONValue.parse(new InputStreamReader(conn.getInputStream()));
        final JSONArray array = (JSONArray) json.get(JsonTags.SLA_SUMMARY_LIST);
        return array;
    }

    private void assertSLAJSONResponse(final JSONArray array, final int startRange, final int endRange, final String jobIDPrefix,
                                       final String jobIDSuffix, final String parentId, final Date startNominalTime,
                                       final String appName, final AppType appType, final Date currentTime,
                                       final List<EventStatus> eventStatuses) throws Exception {
        final Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTime(startNominalTime);
        nominalTime.add(Calendar.HOUR, (startRange - 1));
        int index = 0;
        assertEquals(endRange - (startRange - 1), array.size());
        for (int i = startRange; i <= endRange; i++) {
            final Calendar actualStart = (Calendar) nominalTime.clone();
            actualStart.add(Calendar.MINUTE, i);
            final Calendar expectedEnd = (Calendar) nominalTime.clone();
            expectedEnd.add(Calendar.MINUTE, 60);
            final Calendar actualEnd = (Calendar) expectedEnd.clone();
            actualEnd.add(Calendar.MINUTE, i);
            final JSONObject json = (JSONObject) array.get(index++);
            assertEquals(jobIDPrefix + i + jobIDSuffix, json.get(JsonTags.SLA_SUMMARY_ID));
            assertEquals(parentId, json.get(JsonTags.SLA_SUMMARY_PARENT_ID));
            assertEquals(appName, json.get(JsonTags.SLA_SUMMARY_APP_NAME));
            assertEquals(appType.name(), json.get(JsonTags.SLA_SUMMARY_APP_TYPE));
            assertEquals("RUNNING", json.get(JsonTags.SLA_SUMMARY_JOB_STATUS));
            assertEquals(SLAStatus.IN_PROCESS.name(), json.get(JsonTags.SLA_SUMMARY_SLA_STATUS));

            if (eventStatuses.size() == 0) {
                assertNull(json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS));
            }
            else {
                final StringBuilder expectedEventStatusBuilder = new StringBuilder();
                boolean isFirst = true;
                for (final EventStatus eventStatus : eventStatuses) {
                    if (!isFirst) {
                        expectedEventStatusBuilder.append(",");
                    }
                    isFirst = false;
                    expectedEventStatusBuilder.append(eventStatus);
                }
                assertEquals(expectedEventStatusBuilder.toString(), json.get(JsonTags.SLA_SUMMARY_EVENT_STATUS));
            }

            assertEquals(nominalTime.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_NOMINAL_TIME));
            assertEquals(nominalTime.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_EXPECTED_START));
            assertEquals(actualStart.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_ACTUAL_START));
            assertEquals(expectedEnd.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_EXPECTED_END));
            assertEquals(actualEnd.getTimeInMillis(), json.get(JsonTags.SLA_SUMMARY_ACTUAL_END));
            assertEquals(10L, json.get(JsonTags.SLA_SUMMARY_EXPECTED_DURATION));
            assertEquals(15L, json.get(JsonTags.SLA_SUMMARY_ACTUAL_DURATION));
            assertEquals(currentTime.getTime(), json.get(JsonTags.SLA_SUMMARY_LAST_MODIFIED));
            nominalTime.add(Calendar.HOUR, 1);
        }
    }

    private void insertEntriesIntoSLASummaryTable(final int numEntries, final String jobIDPrefix, final String jobIDSuffix,
                                                  final String parentId, final Date startNominalTime, final String appName,
                                                  final AppType appType, final Date currentTime, final EventStatus eventStatus,
                                                  final SLAStatus slaStatus) throws JPAExecutorException {
        final Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTime(startNominalTime);
        for (int i = 1; i <= numEntries; i++) {
            final Calendar actualStart = (Calendar) nominalTime.clone();
            actualStart.add(Calendar.MINUTE, i);
            final Calendar expectedEnd = (Calendar) nominalTime.clone();
            expectedEnd.add(Calendar.MINUTE, 60);
            final Calendar actualEnd = (Calendar) expectedEnd.clone();
            actualEnd.add(Calendar.MINUTE, i);
            insertEntriesIntoSLASummaryTable(jobIDPrefix + i + jobIDSuffix, parentId, appName, appType, currentTime, eventStatus,
                    slaStatus, nominalTime.getTime(), actualStart.getTime(), ((long) 10), ((long) 15),
                    expectedEnd.getTime(), actualEnd.getTime(), nominalTime.getTime());
            nominalTime.add(Calendar.HOUR, 1);
        }
    }

    private void insertEntriesIntoSLASummaryTable(final String jobID, final String parentId, final String appName,
                                                  final AppType appType, final Date currentTime, final EventStatus eventStatus,
                                                  final SLAStatus slaStatus, final Date expectedStartTime,
                                                  final Date actualStartTime, final long expectedDuration,
                                                  final long actualDuration, final Date expectedEndTime, final Date actualEndTime,
                                                  final Date nominalTime) throws JPAExecutorException {
        final SLASummaryBean bean = new SLASummaryBean();
        bean.setId(jobID);
        bean.setParentId(parentId);
        bean.setAppName(appName);
        bean.setAppType(appType);
        bean.setJobStatus("RUNNING");
        bean.setEventStatus(eventStatus);
        bean.setSLAStatus(slaStatus);
        bean.setNominalTime(nominalTime);
        bean.setExpectedStart(expectedStartTime);
        bean.setActualStart(actualStartTime);
        bean.setExpectedDuration(expectedDuration);
        bean.setActualDuration(actualDuration);
        bean.setExpectedEnd(expectedEndTime);
        bean.setActualEnd(actualEndTime);
        bean.setUser("testuser");
        bean.setLastModifiedTime(currentTime);

        SLASummaryQueryExecutor.getInstance().insert(bean);
    }
}
