package com.xhh.data;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by young on 2018/2/11.
 */
public class GreenplumSync {
    private static final Logger logger = LoggerFactory.getLogger(GreenplumSync.class);

    public static void sync(String sql) throws Exception {
        Class.forName("com.pivotal.jdbc.GreenplumDriver");
        Connection db = DriverManager.getConnection("jdbc:pivotal:greenplum://10.50.8.81:2345;DatabaseName=test", "cctest", "cctest");
        Statement st = db.createStatement();
        st.execute(sql);
        st.close();
    }

    public static String clearSql(String kafkaStr) {
//        String json = "{'database':'test','table':'aaa','type':'update','ts':1518337766,'xid':51232,'commit':true,'data':{'id':123},'old':{'id':1}}";
//
//        String delJson = "{\"database\":\"test\",\"table\":\"aaa\",\"type\":\"delete\",\"ts\":1518337338,\"xid\":51141,\"commit\":true,\"data\":{\"id\":122}}";
//
//        String addJson = "{\"database\":\"test\",\"table\":\"aaa\",\"type\":\"insert\",\"ts\":1518337223,\"xid\":51117,\"commit\":true,\"data\":{\"id\":122}}";
        Structure structure = JSON.parseObject(kafkaStr, Structure.class);

        logger.info("<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        logger.info("data  is" + structure.getData());

        logger.info("database name is " + structure.getDatabase());

        logger.info("table name is " + structure.getTable());

        logger.info("type is " + structure.getType());

        logger.info(structure.getOld());
        logger.info("<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");


        List<String> tables = new ArrayList<>();
        tables.add("fi_user_account_record");
        tables.add("fi_user");
        tables.add("fi_user_account_summary_record");
        if (!structure.getDatabase().equals("percona") && tables.contains(structure.getTable())) {
            Map<String, Object> newData = JSON.parseObject(structure.getData(), Map.class);
            StringBuffer sb = new StringBuffer();
            if (structure.getType().equals("update")) {
                Map<String, Object> oldData = JSON.parseObject(structure.getOld(), Map.class);
                sb.append("update " + structure.getTable());
                sb.append(" set ");
                newData.forEach((k, v) -> {
                    sb.append(k + "='" + v + "',");
                });
                sb.append("1=1" + "  ");

                if (!StringUtils.isEmpty(structure.getOld())) {
                    sb.append(" where ");
                    oldData.forEach((k, v) -> {
                        sb.append(k + "='" + v + "' and ");
                    });
                    sb.append("1=1");
                }
            } else if (structure.getType().equals("insert")) {
                sb.append("insert into " + structure.getTable() + "(");
                newData.forEach((k, v) -> {
                    sb.append(k + ",");
                });
                sb.append(")" + " values(");
                newData.forEach((k, v) -> {
                    sb.append("'" + v + "',");
                });
                sb.append(")");

            } else {
                sb.append("delete from  " + structure.getTable() + " where ");
                newData.forEach((k, v) -> {
                    sb.append(k + "='" + v + "' and ");
                });
                sb.append("1=1");
            }

            if (structure.getType().equals("insert")) {
                logger.info(sb.toString().replace(",)", ")"));
                return sb.toString().replace(",)", ")");
            } else {
                logger.info(sb.toString().replace(",1=1", "").replace("and 1=1", ""));
                return sb.toString().replace(",1=1", "").replace("and 1=1", "");
            }
        } else {
            return null;
        }
    }


}


