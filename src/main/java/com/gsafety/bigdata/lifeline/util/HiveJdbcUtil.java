package com.gsafety.bigdata.lifeline.util;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/8/29.
 */
public class HiveJdbcUtil {
    static {
        ApplicationContext ac = new ClassPathXmlApplicationContext("hive-jdbc.xml");
        jdbcTemplate = ac.getBean("hiveJdbcTemplate",JdbcTemplate.class);
    }

    @Value("${driver}")
    private static final String driver = "org.apache.hive.jdbc.HiveDriver";
    private static final  String url="jdbc:hive2://10.5.4.41:10001";

    private static JdbcTemplate jdbcTemplate ;

    public static void main(String[] args) {
        queryAll();
    }

    public static void queryAll(){
        String sql = "select count(*) from test.water limit 10";
        List<Map<String, Object>> maps = jdbcTemplate.queryForList(sql);
        System.out.println(maps.get(0));
    }

    public void toFile(){
        try {
            Class.forName(driver);
            String url="jdbc:hive2://10.5.4.41:10000";
            Connection connection = DriverManager.getConnection(url);
            String sql = "select * from test.water";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
//            preparedStatement.setString(1,"");
            ResultSet resultSet = preparedStatement.executeQuery();
            System.out.println("........................"+resultSet.getMetaData().getColumnCount());
//            FileInputStream fileInputStream = new FileInputStream(new File("G:\\test.txt"));
//            new BufferedReader(new InputStreamReader(fileInputStream));
            z:for (int j=1;j<100;j++){
                String fileName="G:\\test"+j+".txt";
                File file = new File(fileName);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
                String index="";
                for(int i=1;i<resultSet.getMetaData().getColumnCount();i++){
                    System.out.print(resultSet.getMetaData().getColumnName(i)+"  |  ");
                    index+=resultSet.getMetaData().getColumnName(i)+"  |  ";
                }
                bw.write(index);
                bw.flush();
                String line="";
                while(resultSet.next()){
                    for (int i=1;i<resultSet.getMetaData().getColumnCount();i++){
                        line += resultSet.getObject(i)+"  |  ";
                    }
                    bw.write(line);
                    bw.write("-----------------------------------------------------------------------------------------");
                    bw.newLine();
                    bw.flush();
                    if(file.length()>10*1024*1024)
                        continue z;
                }

            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
