package com.gsafety.bigdata.lifeline.util;




import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/9/6.
 */
public class PhoenixUtil {

    private static final String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static final String url = "jdbc:phoenix:10.5.4.29:2181";
    private static Connection connection = null;
    static{
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection(){
        if(connection==null){
            try {
                connection = DriverManager.getConnection(url);
                String sql = "select * from BRIDGE_HF_JZDL_00000003_1S limit 10";
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                ResultSet resultSet = preparedStatement.executeQuery();
                while(resultSet.next()){
                    System.out.println(resultSet.getString("time"));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static void main(String[] args) {
        getConnection();
    }
}
