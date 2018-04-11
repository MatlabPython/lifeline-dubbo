package com.gsafety.bigdata.lifeline.util;

import java.io.*;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @Author: yifeng G
 * @Date: Create in 10:23 2017/8/21 2017
 * @Description:压缩文件通用类
 * @Modified By:yifeng G
 * @Vsersion:v1.0
 */
public class ZipCompressorUtil {
    static final int BUFFER = 8192;
    private File zipFile;

    public ZipCompressorUtil(String pathName) {
        zipFile = new File(pathName);
        if (!zipFile.exists()) {
            throw new RuntimeException(pathName + "不存在！");
        }
    }

    public void compress(String srcPathName) {
        File file = new File(srcPathName);
        try {
            if (!file.exists())
                throw new RuntimeException(srcPathName + "不存在！");
            FileOutputStream fileOutputStream = new FileOutputStream(zipFile);
      /*CheckedOutputStream:需要维护写入数据校验和的输出流。校验和可用于验证输出数据的完整性。
       * CRC32:
       * 可用于计算数据流的 CRC-32 的类。
       * */
            CheckedOutputStream cos = new CheckedOutputStream(fileOutputStream, new CRC32());
            ZipOutputStream out = new ZipOutputStream(cos);
            //out.setEncoding(System.getProperty("sun.jnu.encoding"));
            String basedir = "";
            compress(file, out, basedir);
            out.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void compress(File file, ZipOutputStream out, String basedir) {
    /* 判断是目录还是文件 */
        if (file.isDirectory()) {
            System.out.println("压缩：" + basedir + file.getName());
            this.compressDirectory(file, out, basedir);
        } else {
            System.out.println("压缩：" + basedir + file.getName());
            this.compressFile(file, out, basedir);
        }
    }

    /**
     * 压缩一个目录
     */
    private void compressDirectory(File dir, ZipOutputStream out, String basedir) {
        if (!dir.exists())
            return;

        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
      /* 递归 */
            compress(files[i], out, basedir + dir.getName() + "/");
        }
    }

    /**
     * 压缩一个文件
     */
    private void compressFile(File file, ZipOutputStream out, String basedir) {
        if (!file.exists()) {
            return;
        }
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            ZipEntry entry = new ZipEntry(basedir + file.getName());
            out.putNextEntry(entry);
            int count;
            byte data[] = new byte[BUFFER];
            while ((count = bis.read(data, 0, BUFFER)) != -1) {
                out.write(data, 0, count);
            }
            bis.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试文件压缩
     * @param args
     */
    public static void main(String[] args) {
        long time1 = System.currentTimeMillis();
        ZipCompressorUtil zc = new ZipCompressorUtil("C:\\123.zip");
        zc.compress("C:\\tmp");
        long time2 = System.currentTimeMillis();
        System.out.println("压缩所需要时间：" + String.valueOf((time2 - time1) / 1000) + "s");
    }
}
