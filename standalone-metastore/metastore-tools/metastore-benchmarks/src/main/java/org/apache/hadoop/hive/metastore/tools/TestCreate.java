package org.apache.hadoop.hive.metastore.tools;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.thrift.TException;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.tools.Util.*;

/**
 * @Author: YKUN
 * @Date: 2024/07/06日  16:00分
 * @Description:
 */
public class TestCreate {
    public static void main(String[] args) throws Exception {
        Map<String, String> parameters = new HashMap<>(40);
        for (int i = 0; i < 2; i++) {
            parameters.put("key" + i, "value" + i);
        }
        for (int i = 1; i <= Integer.parseInt(args[2]); i++) {
            int finalI = i;
            new Thread(() -> {
                System.out.println("Thread" + finalI);
                System.out.println("Start to make sure dbExists");
                try (HMSClient client = new HMSClient(getServerUri(args[0], "9083"), "")) {
                    String dbName = args[1] + finalI;
                    if (!client.dbExists(dbName)) {
                        System.out.println("start  to createDatabase " + dbName);
                        client.createDatabase(dbName);
                        System.out.println("end to createDatabase " + dbName);
                        for (int j = 0; j < 1000; j++) {
                            String tableName = "tname" + j;
                            System.out.println("createTable " + tableName);
                            client.createTable(
                                    new TableBuilder(dbName, tableName)
                                            .withType(TableType.MANAGED_TABLE)
                                            .withColumns(createSchema(Collections.singletonList("name:string")))
                                            .withPartitionKeys(createSchema(Collections.singletonList("date")))
                                            .withParameter("transactional", "true")
                                            .withInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
                                            .withOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
                                            .withSerde("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
                                            .build());
                            System.out.println("addpart");
                            addManyPartitions(client, dbName, tableName, parameters, Collections.singletonList("id"), 40);
                            System.out.println("getTable " + tableName);
                            client.getTable(dbName, tableName);
                            System.out.println("dropDatabase " + dbName);
                            client.dropDatabase(dbName);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }).start();
        }
    }
}

