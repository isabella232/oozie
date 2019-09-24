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

package org.apache.oozie.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.oozie.tools.ECPolicyDisabler.Result;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test for the Erasure Coding disabler code.
 */
public class TestECPolicyDisabler  {

    static abstract class MockDistributedFileSystem extends DistributedFileSystem {
        public abstract void setErasureCodingPolicy(Path path, String policy);
    }

    @Test
    public void testNotSupported() {
        FileSystem fs = mock(FileSystem.class);
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null);
        Assert.assertEquals("result is expected", Result.NOT_SUPPORTED, result);
    }

    @Test
    public void testOkNotChanged() throws IOException {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getErasureCodingPolicy(any())).thenReturn(SystemErasureCodingPolicies.getReplicationPolicy());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null);
        assertEquals("result is expected", Result.ALREADY_SET, result);
        verify(fs).getErasureCodingPolicy(any());
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testOkChanged() throws IOException {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getErasureCodingPolicy(any())).thenReturn(otherErasureCoding());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null);
        assertEquals("result is expected", Result.DONE, result);
        verify(fs).getErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq(ErasureCodeConstants.REPLICATION_POLICY_NAME));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testServerNotSupports() throws IOException {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getErasureCodingPolicy(any())).thenReturn(otherErasureCoding());
        Mockito.doThrow(createNoSuchMethodException()).when(fs).setErasureCodingPolicy(any(), any());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, null);
        assertEquals("result is expected", Result.NO_SUCH_METHOD, result);
        verify(fs).getErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq(ErasureCodeConstants.REPLICATION_POLICY_NAME));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testServerNotSupportsGetErasureCodingPolicyMethod() throws IOException{
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getErasureCodingPolicy(any(Path.class))).thenThrow(createNoSuchMethodException());
        ECPolicyDisabler.Result result = ECPolicyDisabler.check(fs, mock(Path.class));
        assertEquals("result is expected", Result.NO_SUCH_METHOD, result);
        verify(fs).getErasureCodingPolicy(any(Path.class));
        verifyNoMoreInteractions(fs);
    }

    @Test
    public void testOtherRuntimeExceptionThrown() throws IOException {
        MockDistributedFileSystem fs = mock(MockDistributedFileSystem.class);
        when(fs.getErasureCodingPolicy(any())).thenReturn(otherErasureCoding());
        Mockito.doThrow(new RuntimeException("mock io exception")).when(fs).setErasureCodingPolicy(any(), any());
        try {
            ECPolicyDisabler.check(fs, null);
            Assert.fail("exception expected");
        } catch (RuntimeException e) {
            assertNotNull("runtime exception got", e);
        }
        verify(fs).getErasureCodingPolicy(any());
        verify(fs).setErasureCodingPolicy(any(), eq(ErasureCodeConstants.REPLICATION_POLICY_NAME));
        verifyNoMoreInteractions(fs);
    }

    private RuntimeException createNoSuchMethodException() {
        return new RuntimeException(new RemoteException("test", "error", RpcErrorCodeProto.ERROR_NO_SUCH_METHOD));
    }

    private ErasureCodingPolicy otherErasureCoding() {
        return new ErasureCodingPolicy("OTHER", new ECSchema("xxx", 1, 1), 1024, (byte) 0);
    }
}
