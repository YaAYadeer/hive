package org.apache.hadoop.hive.metastore.tools;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.tools.BenchmarkUtils.createManyTables;


public class NONACIDBenchmarks {
    
    private static final Logger LOG = LoggerFactory.getLogger(CoreContext.class);
    
    @State(Scope.Benchmark)
    public static class CoreContext {
        @Param("1")
        protected int howMany;
 
        @State(Scope.Thread)
        public static class ThreadState {
            HMSClient client;

            @Setup
            public void doSetup() throws Exception {
                LOG.debug("Creating client");
                client = HMSConfig.getInstance().newClient();
            }

            @TearDown
            public void doTearDown() throws Exception {
                client.close();
                LOG.debug("Closed a connection to metastore.");
            }
        }

        @Setup
        public void setup() {
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration ctxConfig = ctx.getConfiguration();
            ctxConfig.getLoggerConfig(ACIDBenchmarks.CoreContext.class.getName()).setLevel(Level.INFO);
            ctx.updateLoggers(ctxConfig);
        }
    }

    @State(Scope.Benchmark)
    public static class TestCreate extends NONACIDBenchmarks.CoreContext {

        @State(Scope.Thread)
        public static class ThreadState extends NONACIDBenchmarks.CoreContext.ThreadState {
            @Setup
            public void doSetup() {
                try {
                    client = HMSConfig.getInstance().newClient();
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                }

                try {
                    if (!client.dbExists(dbName)) {
                        client.createDatabase(dbName);
                    }
                } catch (TException e) {
                    LOG.error(e.getMessage());
                }

                LOG.info("creating {} tables", this.howMany);
                createManyTables(client, this.howMany, dbName, tblName);
                for (int i = 0; i < this.howMany; i++) {
                    fullTableNames.add(dbName + ".table_" + i);
                }
            }

            @TearDown
            public void doTearDown() throws Exception {
                
            }
        }

        @Benchmark
        public void openTxn(ACIDBenchmarks.TestOpenTxn.ThreadState state) throws TException {
            state.addTxn(state.client.openTxn(howMany));
            LOG.debug("opened txns, count=", howMany);
        }
    }

}
