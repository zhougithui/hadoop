package com.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ResourceBundle;

/**
 * @author
 */
public class HiveJdbcTest {

    public static void main(String[] args) {
        try {
            String sql = "select * from person";
            ResourceBundle rb = ResourceBundle.getBundle("config");
            Class.forName(rb.getString("hivedriverClassName")).newInstance();

            Connection conn = DriverManager.getConnection(rb.getString("hiveurl"), rb.getString("hiveusername"), rb.getString("hivepassword"));
            java.sql.PreparedStatement pstsm = conn.prepareStatement(sql);
            ResultSet resultSet = pstsm.executeQuery();
            if (resultSet.next()) {
                System.out.println(resultSet.getString(1) + "$$$" + resultSet.getString("name"));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
