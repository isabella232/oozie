/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.tools;

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * Test Dump and dump reading mechanism
 */
public class TestDbLoadDump extends XTestCase {

    File zipDump;

    @BeforeClass
    protected void setUp() throws Exception {
        System.getProperties().remove("oozie.test.config.file");
        super.setUp();
        zipDump = new File(getTestCaseDir() + System.getProperty("file.separator") + "dumpTest.zip");
        ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipDump));
        File dumpFolder = new File(getClass().getResource("/dumpData").getPath());
        IOUtils.zipDir(dumpFolder, "", zos);
    }

    @AfterClass
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testLoadDump() throws Exception {
        OozieDBImportCLI.main(new String[]{"import", zipDump.getAbsolutePath()});
        Services s = new Services();
        s.init();
        EntityManager entityManager = Services.get().get(JPAService.class).getEntityManager();
        Query q = entityManager.createNamedQuery("GET_WORKFLOWS");
        List<WorkflowJobBean> wfjBeans = q.getResultList();
        int wfjSize = wfjBeans.size();
        assertEquals("Workflow not imported", 1, wfjSize);
        assertEquals("Workflow ID incorrect", "0000003-160720041037822-oozie-oozi-W", wfjBeans.get(0).getId());
        assertEquals("AppName incorrect", "aggregator-wf", wfjBeans.get(0).getAppName());

        File newZipDump = new File(getTestCaseDir() + System.getProperty("file.separator") + "newDumpTest.zip");
        //export the contents of the database
        OozieDBExportCLI.main(new String[]{"export", newZipDump.getAbsolutePath()});
        assertEquals("Zip file size differ", zipDump.length(), newZipDump.length());
        ZipFile zip = new ZipFile(newZipDump);
        // check that dump is identical with the original input
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                zip.getInputStream(zip.getEntry("ooziedb_wf.json"))));
        assertTrue("Exported ID incorrect", reader.readLine().contains("0000003-160720041037822-oozie-oozi-W"));
    }
}
