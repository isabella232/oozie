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

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import com.google.common.annotations.VisibleForTesting;
import org.apache.oozie.AppType;
import org.apache.oozie.client.event.SLAEvent;
import org.apache.oozie.client.event.SLAEvent.EventStatus;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.JsonTags;
import org.apache.oozie.client.rest.JsonUtils;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.XLog;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Entity
@Table(name = "SLA_SUMMARY")
@NamedQueries({

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_FOR_SLA_STATUS", query = "update  SLASummaryBean w set w.slaStatus = :slaStatus, w.eventStatus = :eventStatus, w.eventProcessed = :eventProcessed, w.lastModifiedTS = :lastModifiedTS where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_FOR_STATUS_ACTUAL_TIMES", query = "update SLASummaryBean w set w.slaStatus = :slaStatus, w.eventStatus = :eventStatus, w.eventProcessed = :eventProcessed, w.jobStatus = :jobStatus, w.lastModifiedTS = :lastModifiedTS, w.actualStartTS = :actualStartTS, w.actualEndTS = :actualEndTS, w.actualDuration = :actualDuration where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_FOR_ACTUAL_TIMES", query = "update SLASummaryBean w set w.eventProcessed = :eventProcessed, w.actualStartTS = :actualStartTS, w.actualEndTS = :actualEndTS, w.actualEndTS = :actualEndTS, w.actualDuration = :actualDuration, w.lastModifiedTS = :lastModifiedTS where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_EVENTPROCESSED", query = "update SLASummaryBean w set w.eventProcessed = :eventProcessed where w.jobId = :jobId"),

 @NamedQuery(name = "UPDATE_SLA_SUMMARY_ALL", query = "update SLASummaryBean w set w.jobId = :jobId, w.appName = :appName, w.appType = :appType, w.nominalTimeTS = :nominalTime, w.expectedStartTS = :expectedStartTime, w.expectedEndTS = :expectedEndTime, w.expectedDuration = :expectedDuration, w.jobStatus = :jobStatus, w.slaStatus = :slaStatus, w.eventStatus = :eventStatus, w.lastModifiedTS = :lastModTime, w.user = :user, w.parentId = :parentId, w.eventProcessed = :eventProcessed, w.actualDuration = :actualDuration, w.actualEndTS = :actualEndTS, w.actualStartTS = :actualStartTS where w.jobId = :jobId"),

 @NamedQuery(name = "GET_SLA_SUMMARY", query = "select OBJECT(w) from SLASummaryBean w where w.jobId = :id"),

 @NamedQuery(name = "GET_SLA_SUMMARY_RECORDS_RESTART", query = "select OBJECT(w) from SLASummaryBean w where w.eventProcessed <= 7 AND w.lastModifiedTS >= :lastModifiedTime"),

 @NamedQuery(name = "GET_SLA_SUMMARY_EVENTPROCESSED", query = "select w.eventProcessed from SLASummaryBean w where w.jobId = :id"),

 @NamedQuery(name = "GET_SLA_SUMMARY_ALL", query = "select OBJECT(w) from SLASummaryBean w")
})

/**
 * Class to store all the SLA related details (summary) per job
 */
public class SLASummaryBean implements JsonBean {
    private static final XLog LOG = XLog.getLog(SLASummaryBean.class);

    public static final String EVENT_STATUS_SEPARATOR = ",";
    public static final String EVENT_STATUS_ALL = "ALL";

    @Id
    @Basic
    @Column(name = "job_id")
    private String jobId;

    @Basic
    @Index
    @Column(name = "parent_id")
    private String parentId;

    @Basic
    @Index
    @Column(name = "app_name")
    private String appName;

    @Basic
    @Column(name = "app_type")
    private String appType;

    @Basic
    @Column(name = "user_name")
    private String user;

    @Basic
    @Column(name = "created_time")
    private Timestamp createdTimeTS = null;

    @Basic
    @Index
    @Column(name = "nominal_time")
    private Timestamp nominalTimeTS = null;

    @Basic
    @Column(name = "expected_start")
    private Timestamp expectedStartTS = null;

    @Basic
    @Column(name = "expected_end")
    private Timestamp expectedEndTS = null;

    @Basic
    @Column(name = "expected_duration")
    private long expectedDuration = -1;

    @Basic
    @Column(name = "actual_start")
    private Timestamp actualStartTS = null;

    @Basic
    @Column(name = "actual_end")
    private Timestamp actualEndTS = null;

    @Basic
    @Column(name = "actual_duration")
    private long actualDuration = -1;

    @Basic
    @Column(name = "job_status")
    private String jobStatus;

    @Basic
    @Column(name = "event_status")
    private String eventStatus;

    @Basic
    @Column(name = "sla_status")
    private String slaStatus;

    @Basic
    @Index
    @Column(name = "event_processed")
    private byte eventProcessed = 0;

    @Basic
    @Index
    @Column(name = "last_modified")
    private Timestamp lastModifiedTS = null;

    public SLASummaryBean() {
    }

    public SLASummaryBean(SLACalcStatus slaCalc) {
        SLARegistrationBean reg = slaCalc.getSLARegistrationBean();
        setId(slaCalc.getId());
        setAppName(reg.getAppName());
        setAppType(reg.getAppType());
        setNominalTime(reg.getNominalTime());
        setExpectedStart(reg.getExpectedStart());
        setExpectedEnd(reg.getExpectedEnd());
        setExpectedDuration(reg.getExpectedDuration());
        setJobStatus(slaCalc.getJobStatus());
        setSLAStatus(slaCalc.getSLAStatus());
        setEventStatus(slaCalc.getEventStatus());
        setLastModifiedTime(slaCalc.getLastModifiedTime());
        setUser(reg.getUser());
        setParentId(reg.getParentId());
        setEventProcessed(slaCalc.getEventProcessed());
        setActualDuration(slaCalc.getActualDuration());
        setActualEnd(slaCalc.getActualEnd());
        setActualStart(slaCalc.getActualStart());
    }

    public String getId() {
        return jobId;
    }

    public void setId(String jobId) {
        this.jobId = jobId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public Timestamp getCreatedTimestamp() {
        return createdTimeTS;
    }

    public void setCreatedTimestamp(Timestamp createdTime) {
        this.createdTimeTS = createdTime;
    }

    public Date getCreatedTime() {
        return DateUtils.toDate(createdTimeTS);
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTimeTS = DateUtils.convertDateToTimestamp(createdTime);
    }

    public Date getNominalTime() {
        return DateUtils.toDate(nominalTimeTS);
    }

    public Timestamp getNominalTimestamp() {
        return this.nominalTimeTS;
    }

    public void setNominalTime(Date nominalTime) {
        this.nominalTimeTS = DateUtils.convertDateToTimestamp(nominalTime);
    }


    public Date getExpectedStart() {
        return DateUtils.toDate(expectedStartTS);
    }

    public Timestamp getExpectedStartTimestamp() {
        return this.expectedStartTS;
    }

    public void setExpectedStart(Date expectedStart) {
        this.expectedStartTS = DateUtils.convertDateToTimestamp(expectedStart);
    }

    public Date getExpectedEnd() {
        return DateUtils.toDate(expectedEndTS);
    }

    public Timestamp getExpectedEndTimestamp() {
        return this.expectedEndTS;
    }
    public void setExpectedEnd(Date expectedEnd) {
        this.expectedEndTS = DateUtils.convertDateToTimestamp(expectedEnd);
    }

    public long getExpectedDuration() {
        return expectedDuration;
    }

    public void setExpectedDuration(long expectedDuration) {
        this.expectedDuration = expectedDuration;
    }

    public Date getActualStart() {
        return DateUtils.toDate(actualStartTS);
    }

    public Timestamp getActualStartTimestamp() {
        return this.actualStartTS;
    }

    public void setActualStart(Date actualStart) {
        this.actualStartTS = DateUtils.convertDateToTimestamp(actualStart);
    }

    public Date getActualEnd() {
        return DateUtils.toDate(actualEndTS);
    }

    public Timestamp getActualEndTimestamp() {
        return this.actualEndTS;
    }

    public void setActualEnd(Date actualEnd) {
        this.actualEndTS = DateUtils.convertDateToTimestamp(actualEnd);
    }

    public long getActualDuration() {
        return actualDuration;
    }

    public void setActualDuration(long actualDuration) {
        this.actualDuration = actualDuration;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String status) {
        this.jobStatus = status;
    }

    public SLAEvent.EventStatus getEventStatus() {
        return (eventStatus != null ? SLAEvent.EventStatus.valueOf(eventStatus) : null);
    }

    public void setEventStatus(SLAEvent.EventStatus eventStatus) {
        this.eventStatus = (eventStatus != null ? eventStatus.name() : null);
    }

    public SLAEvent.SLAStatus getSLAStatus() {
        return (slaStatus != null ? SLAEvent.SLAStatus.valueOf(slaStatus) : null);
    }

    public String getSLAStatusString() {
        return slaStatus;
    }

    public String getEventStatusString() {
        return eventStatus;
    }

    public void setSLAStatus(SLAEvent.SLAStatus stage) {
        this.slaStatus = (stage != null ? stage.name() : null);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public AppType getAppType() {
        return AppType.valueOf(appType);
    }

    public void setAppType(AppType appType) {
        this.appType = appType.toString();
    }

    public byte getEventProcessed() {
        return eventProcessed;
    }

    public void setEventProcessed(int eventProcessed) {
        this.eventProcessed = (byte)eventProcessed;
    }

    public Date getLastModifiedTime() {
        return DateUtils.toDate(lastModifiedTS);
    }

    public Timestamp getLastModifiedTimestamp() {
        return this.lastModifiedTS;
    }

    public void setLastModifiedTime(Date lastModified) {
        this.lastModifiedTS = DateUtils.convertDateToTimestamp(lastModified);
    }

    @SuppressWarnings("unchecked")
    @Override
    public JSONObject toJSONObject() {
        final JSONObject json = new JSONObject();

        json.put(JsonTags.SLA_SUMMARY_ID, jobId);
        if (parentId != null) {
            json.put(JsonTags.SLA_SUMMARY_PARENT_ID, parentId);
        }
        json.put(JsonTags.SLA_SUMMARY_APP_NAME, appName);
        json.put(JsonTags.SLA_SUMMARY_APP_TYPE, appType);
        json.put(JsonTags.SLA_SUMMARY_USER, user);
        json.put(JsonTags.SLA_SUMMARY_NOMINAL_TIME, nominalTimeTS.getTime());
        if (expectedStartTS != null) {
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, expectedStartTS.getTime());
        }
        else {
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, null);
        }
        if (actualStartTS != null) {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, actualStartTS.getTime());
        }
        else {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, null);
        }
        json.put(JsonTags.SLA_SUMMARY_EXPECTED_END, expectedEndTS.getTime());
        if (actualEndTS != null) {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, actualEndTS.getTime());
        }
        else {
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, null);
        }
        json.put(JsonTags.SLA_SUMMARY_EXPECTED_DURATION, expectedDuration);
        json.put(JsonTags.SLA_SUMMARY_ACTUAL_DURATION, actualDuration);
        json.put(JsonTags.SLA_SUMMARY_JOB_STATUS, jobStatus);
        json.put(JsonTags.SLA_SUMMARY_SLA_STATUS, slaStatus);
        json.put(JsonTags.SLA_SUMMARY_EVENT_STATUS, new SLAEventStatusCalculator(this).calculate().toString());
        json.put(JsonTags.SLA_SUMMARY_LAST_MODIFIED, lastModifiedTS.getTime());

        return json;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        if (timeZoneId == null) {
            return toJSONObject();
        }
        else {
            JSONObject json = new JSONObject();
            json.put(JsonTags.SLA_SUMMARY_ID, jobId);
            if (parentId != null) {
                json.put(JsonTags.SLA_SUMMARY_PARENT_ID, parentId);
            }
            json.put(JsonTags.SLA_SUMMARY_APP_NAME, appName);
            json.put(JsonTags.SLA_SUMMARY_APP_TYPE, appType);
            json.put(JsonTags.SLA_SUMMARY_USER, user);
            json.put(JsonTags.SLA_SUMMARY_NOMINAL_TIME, JsonUtils.formatDateRfc822(nominalTimeTS, timeZoneId));
            if (expectedStartTS != null) {
                json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, JsonUtils.formatDateRfc822(expectedStartTS, timeZoneId));
            }
            else {
                json.put(JsonTags.SLA_SUMMARY_EXPECTED_START, null);
            }
            if (actualStartTS != null) {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, JsonUtils.formatDateRfc822(actualStartTS, timeZoneId));
            }
            else {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_START, null);
            }
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_END, JsonUtils.formatDateRfc822(expectedEndTS, timeZoneId));
            if (actualEndTS != null) {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, JsonUtils.formatDateRfc822(actualEndTS, timeZoneId));
            }
            else {
                json.put(JsonTags.SLA_SUMMARY_ACTUAL_END, null);
            }
            json.put(JsonTags.SLA_SUMMARY_EXPECTED_DURATION, expectedDuration);
            json.put(JsonTags.SLA_SUMMARY_ACTUAL_DURATION, actualDuration);
            json.put(JsonTags.SLA_SUMMARY_JOB_STATUS, jobStatus);
            json.put(JsonTags.SLA_SUMMARY_SLA_STATUS, slaStatus);
            json.put(JsonTags.SLA_SUMMARY_EVENT_STATUS, new SLAEventStatusCalculator(this).calculate().toString());
            json.put(JsonTags.SLA_SUMMARY_LAST_MODIFIED, JsonUtils.formatDateRfc822(lastModifiedTS, timeZoneId));

            return json;
        }
    }

    @VisibleForTesting
    static class SLAEventStatusCalculator {
        private final Set<EventStatus> events = new LinkedHashSet<>();
        private final SLASummaryBean slaSummaryBean;

        SLAEventStatusCalculator(final SLASummaryBean slaSummaryBean) {
            this.slaSummaryBean = slaSummaryBean;
        }

        SLAEventStatusCalculator calculate() {
            events.clear();

            addStartEvents();

            addDurationEvents();

            addEndEvents();

            return this;
        }

        private void addStartEvents() {
            if (slaSummaryBean.expectedStartTS != null) {
                if (slaSummaryBean.actualStartTS != null) {
                    final long diff = (slaSummaryBean.actualStartTS.getTime() - slaSummaryBean.expectedStartTS.getTime()) / 60_000;
                    if (diff > 0) {
                        events.add(EventStatus.START_MISS);
                    }
                    else {
                        events.add(EventStatus.START_MET);
                    }
                }
                else {
                    final long diff = (nowMs() - slaSummaryBean.expectedStartTS.getTime()) / 60_000;
                    if (diff > 0) {
                        events.add(EventStatus.START_MISS);
                    }
                }
            }
        }

        private long nowMs() {
            return new Date().getTime();
        }

        private void addDurationEvents() {
            if (slaSummaryBean.expectedDuration != -1) {
                if (slaSummaryBean.actualDuration != -1) {
                    final long diff = slaSummaryBean.actualDuration - slaSummaryBean.expectedDuration;
                    if (diff > 0) {
                        events.add(EventStatus.DURATION_MISS);
                    }
                    else {
                        events.add(EventStatus.DURATION_MET);
                    }
                }
                else {
                    if (slaSummaryBean.actualStartTS != null) {
                        final long currentDur = nowMs() - slaSummaryBean.actualStartTS.getTime();
                        if (slaSummaryBean.expectedDuration < currentDur) {
                            events.add(EventStatus.DURATION_MISS);
                        }
                    }
                }
            }
        }

        public void addEndEvents() {
            if (slaSummaryBean.expectedEndTS != null) {
                if (slaSummaryBean.actualEndTS != null) {
                    final long diff = (slaSummaryBean.actualEndTS.getTime() - slaSummaryBean.expectedEndTS.getTime()) / 60_000;
                    if (diff > 0) {
                        events.add(EventStatus.END_MISS);
                    }
                    else {
                        events.add(EventStatus.END_MET);
                    }
                }
                else {
                    final long diff = (nowMs() - slaSummaryBean.expectedEndTS.getTime()) / 60_000;
                    if (diff > 0) {
                        events.add(EventStatus.END_MISS);
                    }
                }
            }
        }

        @Override
        public String toString() {
            final StringBuilder eventStatusBuilder = new StringBuilder();

            boolean first = true;
            for (final EventStatus e: events) {
                if (!first) {
                    eventStatusBuilder.append(EVENT_STATUS_SEPARATOR);
                }

                eventStatusBuilder.append(e.toString());
                first = false;
            }

            return eventStatusBuilder.toString();
        }
    }

    /**
     * Convert a sla summary list into a json object.
     *
     * @param slaSummaryList sla summary list.
     * @param timeZoneId time zone to use for dates in the JSON array.
     * @param includeEventStatus whether to include {@link SLASummaryBean#eventStatus} in the JSON object
     * @return the corresponding JSON object
     */
    @SuppressWarnings("unchecked")
    public static JSONObject toJSONObject(final List<? extends SLASummaryBean> slaSummaryList,
                                          final String timeZoneId,
                                          final boolean includeEventStatus) {

        LOG.debug("Transforming to JSON object. [slaSummaryList.size={0};timeZoneId={1};includeEventStatus={2}]",
                slaSummaryList.size(),
                timeZoneId,
                includeEventStatus);

        final JSONObject jsonObject = new JSONObject();
        final JSONArray jsonArray = new JSONArray();

        for (final SLASummaryBean summary : slaSummaryList) {
            final JSONObject summaryJson = summary.toJSONObject(timeZoneId);

            final boolean removeEventStatus = !includeEventStatus && summaryJson.containsKey(JsonTags.SLA_SUMMARY_EVENT_STATUS);
            if (removeEventStatus) {
                LOG.trace("Removing event status. [{0}={1}]",
                        JsonTags.SLA_SUMMARY_ID,
                        summaryJson.get(JsonTags.SLA_SUMMARY_ID));

                summaryJson.remove(JsonTags.SLA_SUMMARY_EVENT_STATUS);
            }

            jsonArray.add(summaryJson);
        }

        jsonObject.put(JsonTags.SLA_SUMMARY_LIST, jsonArray);

        LOG.debug("Returning JSON object. [jsonArray.size={0}]", jsonArray.size());

        return jsonObject;
    }

}
