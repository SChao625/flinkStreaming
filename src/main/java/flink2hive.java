
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class flink2hive {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "E:\\idea\\IDEApro\\src\\main\\conf"; // a local path
        String version         = "1.1.0";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

        Table sql = tableEnv.sqlQuery("select * from dept");

        tableEnv.toAppendStream(sql, Row.class).print();

        try {
            tableEnv.execute("adas");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
