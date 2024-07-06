package org.apache.hadoop.hive.metastore.tools;

import java.net.URISyntaxException;

import static org.apache.hadoop.hive.metastore.tools.Util.getServerUri;

/**
 * @Author: YKUN
 * @Date: 2024/07/06日  16:00分
 * @Description:
 */
public class TestCreate {
    public static void main(String[] args) throws Exception {
        try (HMSClient client = new HMSClient(getServerUri(args[0],"9083"), "")) {
            System.out.println("Start to make sure dbExists");
            if (!client.dbExists(args[1])) {
                System.out.println("=start  to createDatabase");
                client.createDatabase(args[1]);
                System.out.println("end to createDatabase");
            }
    }
}
}
