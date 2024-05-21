/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Formatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.metastore.tools.HMSBenchmarks.*;
import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;

public class NONACIDThread implements Runnable{
    
    private static final Logger LOG = LoggerFactory.getLogger(NONACIDThread.class);
    // HMS地址  14
    private String host;
    private Integer port;
    private String confDir;
    // 表数 分区数
    private int[] instances;
    private int tnum;
    private int[] nParameters;
    // 必备成员
    private BenchData bData;
    private MicroBenchmark bench;
    private StringBuilder sb = new StringBuilder();
    private BenchmarkSuite suite = new BenchmarkSuite();
    // 进程相关
    private CountDownLatch startDownLatch;
    private CountDownLatch endCountDownLatch;
    private Pattern[] matches;
    private Pattern[] exclude;
    // 结果文件
    private String dataSaveDir;
    private String outputFile;
    
    public NONACIDThread(CountDownLatch startDownLatch, CountDownLatch endCountDownLatch,
                         String host,Integer port,String confDir,
                         int[] instances, String dbName,String tableName,int tnum,int[] nParameters,
                         int warmup,int spinCount,String dataSaveDir,String outputFile, boolean doSanitize,Pattern[] matches,Pattern[] exclude) {
        this.startDownLatch = startDownLatch;
        this.endCountDownLatch = endCountDownLatch;
        this.host=host;
        this.port=port;
        this.confDir=confDir;
        this.instances = instances;
        this.nParameters = nParameters;
        this.bData = new BenchData(dbName, tableName);
        this.bench = new MicroBenchmark(warmup, spinCount);
        this.tnum=tnum;
        this.matches = matches;
        this.exclude = exclude;
        this.dataSaveDir=dataSaveDir;
        this.outputFile = outputFile;
        suite.setScale(TimeUnit.MILLISECONDS).doSanitize(doSanitize);
    }
    
    public void setup() {
        //howmany是分区数   tnum是表数
        for (int howMany: instances) {
            suite.add("getTable", () -> benchmarkGetTable(bench, bData))
                      .add("get_databases", () -> benchmarkListDatabases(bench, bData))
                      .add("get_database",() -> benchmarkGetDatabase(bench, bData));
                       
//                    .add("create_table",() -> benchmarkCreateTables(bench, bData,tnum));
//                    .add("testadd",()-> benchmarkAddParation(bench, bData,howMany, 2));             
//                    .add("get_partitions_by_names" + '.' + howMany, () -> benchmarkGetPartitionsByName(bench, bData, howMany))             
//                    .add("get_partition_names" + '.' + howMany, () -> benchmarkGetPartitionNames(bench, bData, howMany));

        }
    }
    public void testcase() {
        try (HMSClient client = new HMSClient(getServerUri(host, String.valueOf(port)), confDir)) {
            bData.setClient(client);
            // 创建指定数据库
            if (!client.dbExists(bData.dbName)) {
                client.createDatabase(bData.dbName);
            }

            if (client.tableExists(bData.dbName, bData.tableName)) {
                client.dropTable(bData.dbName, bData.tableName);
            }

            // Arrange various benchmarks in a suite
            BenchmarkSuite result = suite.runMatching(matches, exclude);

            Formatter fmt = new Formatter(sb);
            // 展示为csv形式
            result.displayCSV(fmt,BenchmarkTool.CSV_SEPARATOR);

            PrintStream output = System.out;
            if (outputFile != null) {
                output = new PrintStream(outputFile);
            }

            if (outputFile != null) {
                // Print results to stdout as well
                StringBuilder s = new StringBuilder();
                Formatter f = new Formatter(s);
                result.display(f);
                System.out.print(s);
                f.close();
            }
            
            output.print(sb.toString());
            fmt.close();
            output.close();

            if (dataSaveDir != null) {
                BenchmarkTool.saveData(result.getResult(), dataSaveDir, TimeUnit.MILLISECONDS);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
    //  每个thread创建的 db 和table得区分开，结果文件怎么输出？是每个线程的均值，还是线程和？
    //  run操作类型
    public void run() {
        setup();
        LOG.info("Using table '{}.{}",bData.dbName, bData.tableName);
        try {
            synchronized (startDownLatch) {
                startDownLatch.countDown();
            }
            startDownLatch.await();
            testcase();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            endCountDownLatch.countDown();
        }
    }
}
