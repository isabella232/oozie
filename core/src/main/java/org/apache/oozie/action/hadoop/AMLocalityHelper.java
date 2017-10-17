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

package org.apache.oozie.action.hadoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.oozie.util.XLog;

import com.google.common.collect.Sets;

public final class AMLocalityHelper {
    private AMLocalityHelper() {}  // no instances

    private static final RecordFactory recordFactory = RecordFactoryProvider
            .getRecordFactory(null);
    private static final XLog LOG = XLog.getLog(AMLocalityHelper.class);

    private static final String DEFAULT_RACK = "/default-rack";
    private static final String RACK_GROUP = "rack";
    private static final String NODE_IF_RACK_GROUP = "node1";
    private static final String NODE_IF_NO_RACK_GROUP = "node2";

    // Matches patterns like "node", "/rack", or "/rack/node"
    private static final Pattern RACK_NODE_PATTERN =
            Pattern.compile(
                String.format("(?<%s>[^/]+?)|(?<%s>/[^/]+?)(?:/(?<%s>[^/]+?))?",
                NODE_IF_NO_RACK_GROUP, RACK_GROUP, NODE_IF_RACK_GROUP));

    // Based on MAPREDUCE-6871
    public static List<ResourceRequest> generateResourceRequests(int memory, int vcores, int priority,
            Collection<String> locality) {
        Resource capability = recordFactory.newRecordInstance(Resource.class);
        capability.setMemorySize(memory);
        capability.setVirtualCores(vcores);
        LOG.debug("AppMaster capability = {0}", capability);

        List<ResourceRequest> amResourceRequests = new ArrayList<>();
        // Always have an ANY request
        ResourceRequest amAnyResourceRequest =
                createSingleContainerRelaxedLocalityResourceRequest(ResourceRequest.ANY, capability, priority);
        Map<String, ResourceRequest> rackRequests = new HashMap<>();
        amResourceRequests.add(amAnyResourceRequest);

        for (String amStrictResource : locality) {
            amAnyResourceRequest.setRelaxLocality(false);
            Matcher matcher = RACK_NODE_PATTERN.matcher(amStrictResource);
            if (matcher.matches()) {
                String nodeName;
                String rackName = matcher.group(RACK_GROUP);
                if (rackName == null) {
                    rackName = DEFAULT_RACK;
                    nodeName = matcher.group(NODE_IF_NO_RACK_GROUP);
                } else {
                    nodeName = matcher.group(NODE_IF_RACK_GROUP);
                }
                ResourceRequest amRackResourceRequest = rackRequests.get(rackName);
                if (amRackResourceRequest == null) {
                    amRackResourceRequest = createSingleContainerRelaxedLocalityResourceRequest(rackName, capability, priority);
                    amResourceRequests.add(amRackResourceRequest);
                    rackRequests.put(rackName, amRackResourceRequest);
                }
                if (nodeName != null) {
                    amRackResourceRequest.setRelaxLocality(false);
                    ResourceRequest amNodeResourceRequest =
                            createSingleContainerRelaxedLocalityResourceRequest(nodeName, capability, priority);
                    amResourceRequests.add(amNodeResourceRequest);
                }
            } else {
                String errMsg =
                        "Invalid resource name: " + amStrictResource + " specified.";
                LOG.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
        }

        for (ResourceRequest amResourceRequest : amResourceRequests) {
            LOG.debug("ResourceRequest: resource = {0}, locality = {1}",
                    amResourceRequest.getResourceName(),
                    amResourceRequest.getRelaxLocality());
        }

        return amResourceRequests;
    }

    private static ResourceRequest createSingleContainerRelaxedLocalityResourceRequest(String resource,
            Resource capability, int priority) {
        ResourceRequest resourceRequest =
                recordFactory.newRecordInstance(ResourceRequest.class);
        resourceRequest.setPriority(Priority.newInstance(priority));
        resourceRequest.setResourceName(resource);
        resourceRequest.setCapability(capability);
        resourceRequest.setNumContainers(1);
        resourceRequest.setRelaxLocality(true);
        return resourceRequest;
    }
}
