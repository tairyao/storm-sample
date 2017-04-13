package com.tencent.tairyao.storm.util;

import java.sql.*;
import java.util.HashMap;

/**
 * Created by tairyao on 2017/2/11.
 */
public class MysqlUtil {

    private Connection conn;

    public MysqlUtil() {
        this.connectDb();
    }

    private void connectDb() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            this.conn = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/storm", "root", "");
        } catch (Exception e) {
            System.out.println("数据库连接失败" + e.getMessage());
        }
    }

    public boolean insertResult(String sql, String time, int value) {
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, time);
            ps.setInt(2, value);
            int count = ps.executeUpdate();
            System.out.println("更新 " + count + " 条数据");
        } catch (SQLException e) {
            System.out.println("sql执行失败" + e.getMessage());
            return false;
        }
        return true;
    }

    public HashMap<String, Object> getResult(String sql) {
        HashMap<String, Object> result = new HashMap<String, Object>();
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            rs.next();
            result.put("time", rs.getString("time"));
            result.put("success", rs.getInt("success"));
        } catch (SQLException e) {
            System.out.println("sql执行失败" + e.getMessage());
        }
        return result;
    }

}
