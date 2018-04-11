package com.gsafety.bigdata.lifeline.util.zk;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import org.apache.commons.lang.StringUtils;

import java.io.*;

/**
 * @Author: yifeng G
 * @Date: Create in 10:37 2017/10/27 2017
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class RemoteExecuteCommand {
    //字符编码默认是utf-8
    private static String DEFAULTCHART = "UTF-8";
    public static Connection conn;
    private String ip;
    private String userName;
    private String userPwd;

    public RemoteExecuteCommand(String ip, String userName, String userPwd) {
        this.ip = ip;
        this.userName = userName;
        this.userPwd = userPwd;
    }


    public RemoteExecuteCommand() {

    }

    /**
     * 远程登录linux的主机
     *
     * @return 登录成功返回true，否则返回false
     * @author Ickes
     * @since V0.1
     */
    public Boolean login() {
        boolean flg = false;
        try {
            conn = new Connection(ip);
            conn.connect();//连接
            flg = conn.authenticateWithPassword(userName, userPwd);//认证
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flg;
    }

    /**
     * @param cmd 即将执行的命令
     * @return 命令执行完后返回的结果值
     * @author Ickes
     * 远程执行shll脚本或者命令
     * @since V0.1
     */
    public String execute(String cmd) {
        String result = "";
        try {
            if (login()) {
                Session session = conn.openSession();//打开一个会话
                session.execCommand(cmd);//执行命令
                result = processStdout(session.getStdout(), DEFAULTCHART);
                //如果为得到标准输出为空，说明脚本执行出错了
                if (StringUtils.isBlank(result)) {
                    result = processStdout(session.getStderr(), DEFAULTCHART);
                }
                conn.close();
                session.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    /**
     * @param cmd 即将执行的命令
     * @return 命令执行成功后返回的结果值，如果命令执行失败，返回空字符串，不是null
     * @author Ickes
     * 远程执行shll脚本或者命令
     * @since V0.1
     */
    public String executeSuccess(String cmd) {
        String result = "";
        try {
            if (login()) {
                Session session = conn.openSession();//打开一个会话
                session.execCommand(cmd);//执行命令
                result = processStdout(session.getStdout(), DEFAULTCHART);
                conn.close();
                session.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 解析脚本执行返回的结果集
     *
     * @param in      输入流对象
     * @param charset 编码
     * @return 以纯文本的格式返回
     * @author Ickes
     * @since V0.1
     */
    private String processStdout(InputStream in, String charset) {
        InputStream stdout = new StreamGobbler(in);
        StringBuffer buffer = new StringBuffer();
        ;
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout, charset));
            String line = null;
            while ((line = br.readLine()) != null) {
                buffer.append(line + "\n");
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }

    public static void setCharset(String charset) {
        DEFAULTCHART = charset;
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPwd() {
        return userPwd;
    }

    public void setUserPwd(String userPwd) {
        this.userPwd = userPwd;
    }

    public static void main(String[] args) {
//        RemoteExecuteCommand rec=new RemoteExecuteCommand("10.5.4.112", "zone","aB.123456");
//        //执行命令
//        System.out.println(rec.execute("ifconfig"));
//        //执行脚本
////        rec.execute("sh /usr/local/tomcat/bin/statup.sh");
//        //这个方法与上面最大的区别就是，上面的方法，不管执行成功与否都返回，
//        //这个方法呢，如果命令或者脚本执行错误将返回空字符串
//        rec.executeSuccess("ifconfig");
        ch.ethz.ssh2.Connection conn = new ch.ethz.ssh2.Connection("10.5.4.40");
        try {
            conn.connect();
            if (!conn.authenticateWithPassword("root", "aB.123456")) {
                return;
            }

            ch.ethz.ssh2.Session ssh = conn.openSession();
            String cmd = "ifconfig";
//            String cmd = "date";
            ssh.execCommand(cmd);
            InputStream stdout = new StreamGobbler(ssh.getStdout());
            BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
            StringBuffer stringBuffer = new StringBuffer();
            String result = "";
            while ((result = stdoutReader.readLine()) != null) {
                stringBuffer.append(result);
            }
            System.out.println(stringBuffer.toString());
            stdoutReader.close();
            stdout.close();
            ssh.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
