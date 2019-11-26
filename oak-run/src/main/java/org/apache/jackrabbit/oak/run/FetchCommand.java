/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.run;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.LastRevRecoveryAgent;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.util.MapDBMapFactory;
import org.apache.jackrabbit.oak.plugins.document.util.MapFactory;
import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;

import com.google.common.io.Closer;

class FetchCommand implements Command {

    MapFactory oldf = MapFactory.getInstance();

    @Override
    public void execute(String... args) throws Exception {
    	Closer closer = Closer.create();
        MapDBMapFactory mdbmf = new MapDBMapFactory();
        closer.register(mdbmf);
        MapFactory.setInstance(mdbmf);
        String h = "recovery mongodb://host:port/database|jdbc:... { dryRun }";

        try {
            DocumentNodeStoreBuilder<?> builder = Utils.createDocumentMKBuilder(args, closer, h);

            if (builder == null) {
                System.err.println("Recovery only available for DocumentNodeStore backed by MongoDB or RDB persistence");
                System.exit(1);
            }

            // dryRun implies readonly repo
            boolean dryRun = Arrays.asList(args).contains("dryRun");
            if (dryRun) {
                builder.setReadOnlyMode();
            }

            DocumentNodeStore dns = builder.build();
            closer.register(Utils.asCloseable(dns));

            DocumentStore ds = builder.getDocumentStore();

            LastRevRecoveryAgent agent = null;
            MissingLastRevSeeker seeker = null;

            if (ds instanceof MongoDocumentStore) {
                MongoDocumentStore docStore = (MongoDocumentStore) ds;
                agent = new LastRevRecoveryAgent(dns);
                seeker = new MongoMissingLastRevSeeker(docStore, dns.getClock());
            } else if (ds instanceof RDBDocumentStore) {
                RDBDocumentStore docStore = (RDBDocumentStore) ds;
                agent = new LastRevRecoveryAgent(dns);
                seeker = new RDBMissingLastRevSeeker(docStore, dns.getClock());
            }

            if (agent == null || seeker == null) {
                System.err.println("Recovery only available for MongoDocumentStore and RDBDocumentStore (this: "
                        + ds.getClass().getName() + ")");
                System.exit(1);
            }

            if (builder.getClusterId() == 0) {
                System.err.println("Please specify the clusterId for recovery using --clusterId");
                try {
                    List<ClusterNodeInfoDocument> all = ClusterNodeInfoDocument.all(ds);
                    System.err.println("Existing entries in the clusternodes collection are:");
                    for (ClusterNodeInfoDocument c : all) {
                        String state = c.isActive() ? "ACTIVE" : "INACTIVE";
                        System.err.println(c.getClusterId() +
                                " (" + state + "): " +
                                c.toString().replace('\n', ' '));
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace(System.err);
                }
                System.exit(1);
            }

            Iterable<NodeDocument> docs = seeker.getCandidates(0);
            if (docs instanceof Closeable) {
                closer.register((Closeable) docs);
            }
            List<String> inconsistentPaths = new ArrayList<String>();
            
            inconsistentPaths = agent.getInconsistentDocuments(docs, builder.getClusterId());
            

            System.out.println("Inconsistent paths are as follows");
            
            String csvFileDir = System.getProperty("user.dir");
            String fileName = csvFileDir+File.separator+"test.csv";
            try {
    			BufferedWriter outputWriter = new BufferedWriter(new FileWriter(fileName));
    			for(String path : inconsistentPaths) {
    				System.out.println(path);
    	        	StringBuilder sb = new StringBuilder();
    	        	sb.append(path).append(",").append(builder.getClusterId()).append("\n");
    	        	outputWriter.write(sb.toString());
    	        }
    			outputWriter.close();
    		} catch (IOException e) {
    			e.printStackTrace();
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
            MapFactory.setInstance(oldf);
        }
    }
}
