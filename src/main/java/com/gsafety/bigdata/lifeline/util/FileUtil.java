package com.gsafety.bigdata.lifeline.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author: yifeng G
 * @Date: Create in 11:26 2017/8/21 2017
 * @Description:
 * @Modified By:
 * @Vsersion:v1.0
 */
public class FileUtil {
    static Logger logger = LoggerFactory.getLogger(FileUtil.class);
    private static final List<String> list = new ArrayList<String>();
    public static String currentWorkDir = System.getProperty("user.dir");
    private static final int blockFileSize = 1024 * 1024 * 50;// 按照文件大小进行拆分
    public static int countThread = 0;

    /**
     * 删除某个文件夹下的所有文件夹和文件
     *
     * @param delpath String
     * @return boolean
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static boolean deletefile(String delpath) throws Exception {
        try {
            File file = new File(delpath);
            // 当且仅当此抽象路径名表示的文件存在且 是一个目录时，返回 true
            if (!file.isDirectory()) {
                file.delete();
            } else if (file.isDirectory()) {
                String[] filelist = file.list();
                for (int i = 0; i < filelist.length; i++) {
                    File delfile = new File(delpath + "/" + filelist[i]);
                    if (!delfile.isDirectory()) {
                        delfile.delete();
                        System.out.println(delfile.getAbsolutePath() + "删除文件成功");
                    } else if (delfile.isDirectory()) {
                        deletefile(delpath + "/" + filelist[i]);
                    }
                }
                System.out.println(file.getAbsolutePath() + "删除成功");
                file.delete();
            }

        } catch (FileNotFoundException e) {
            System.out.println("deletefile() Exception:" + e.getMessage());
        }
        return true;
    }

    /**
     * 输出某个文件夹下的所有文件夹和文件路径
     *
     * @param filepath String
     * @return boolean
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static boolean readfile(String filepath) throws FileNotFoundException, IOException {
        try {
            File file = new File(filepath);
            System.out.println("遍历的路径为：" + file.getAbsolutePath());
            // 当且仅当此抽象路径名表示的文件存在且 是一个目录时（即文件夹下有子文件时），返回 true
            if (!file.isDirectory()) {
                System.out.println("该文件的绝对路径：" + file.getAbsolutePath());
                System.out.println("名称：" + file.getName());
            } else if (file.isDirectory()) {
                // 得到目录中的文件和目录
                String[] filelist = file.list();
                if (filelist.length == 0) {
                    System.out.println(file.getAbsolutePath() + "文件夹下，没有子文件夹或文件");
                } else {
                    System.out.println(file.getAbsolutePath() + "文件夹下，有子文件夹或文件");
                }
                for (int i = 0; i < filelist.length; i++) {
                    File readfile = new File(filepath + "/" + filelist[i]);
                    System.out.println("遍历的路径为：" + readfile.getAbsolutePath());
                    if (!readfile.isDirectory()) {
                        System.out.println("该文件的路径：" + readfile.getAbsolutePath());
                        System.out.println("名称：" + readfile.getName());
                    } else if (readfile.isDirectory()) {
                        System.out.println("-----------递归循环-----------");
                        readfile(filepath + "/" + filelist[i]);
                    }
                }
            }

        } catch (FileNotFoundException e) {
            System.out.println("readfile() Exception:" + e.getMessage());
        }
        return true;
    }


    /**
     * @param directory
     * @describe创建多层级目录
     */
    public static boolean createFileDirectory(String directory) {
        File file = new File(directory);
        if (!file.exists()) {
            file.mkdirs();
            System.out.println("创建文件夹路径为：" + directory);
        } else {
            System.out.println("该文件夹已经创建成功......");
        }
        return true;
    }

    /**
     * @param path
     * @describe 根据路径获取文件夹下所有文件的名字
     */
    public static List getFileName(String path) {
        // get file list where the path has
        File file = new File(path);
        // get the folder list
        File[] array = file.listFiles();
        for (int i = 0; i < array.length; i++) {
            if (array[i].isFile()) {
                // only take file name
                System.out.println(array[i].getName());
                // take file path and name
                System.out.println(array[i]);
                // take file path and name
                System.out.println(array[i].getPath());
                list.add(i, array[i].getName());
            } else if (array[i].isDirectory()) {
                getFileName(array[i].getPath());
            }
        }
        return list;
    }

    /**
     * @param source
     * @param target
     * @describe 从一个目录复制文件到一个另一个目录下面
     */
    public static void fileChannelCopy(File source, File target) {
        try {
            FileInputStream fi = null;
            FileOutputStream fo = null;
            FileChannel in = null;
            FileChannel out = null;
            if (source.isDirectory()) {
                for (int i = 0; i < source.length(); i++) {
                    fi = new FileInputStream(source.listFiles()[i]);
                    fo = new FileOutputStream(target);
                    in = fi.getChannel();//得到对应的文件通道
                    out = fo.getChannel();//得到对应的文件通道
                    in.transferTo(0, in.size(), out);//连接两个通道，并且从in通道读取，然后写入out通道
                    fi.close();
                    in.close();
                    fo.close();
                    out.close();
                }
            } else {

            }
        } catch (IOException e) {
            e.printStackTrace();
//        } finally {
//            try {
//                fi.close();
//                in.close();
//                fo.close();
//                out.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
    }

    /**
     * @param str
     * @param length
     * @param ch
     * @return
     * @describe 左填充
     */
    public static String leftPad(String str, int length, char ch) {
        if (str.length() >= length) {
            return str;
        }
        char[] chs = new char[length];
        Arrays.fill(chs, ch);
        char[] src = str.toCharArray();
        System.arraycopy(src, 0, chs, length - src.length, src.length);
        return new String(chs);

    }

    /***
     * @describe 递归获取指定目录下的所有的文件（不包括文件夹）
     * @param
     * @return
     */
    public static ArrayList<File> getAllFiles(String dirPath) {
        File dir = new File(dirPath);
        ArrayList<File> files = new ArrayList<File>();
        if (dir.isDirectory()) {
            File[] fileArr = dir.listFiles();
            for (int i = 0; i < fileArr.length; i++) {
                File f = fileArr[i];
                if (f.isFile()) {
                    files.add(f);
                } else {
                    files.addAll(getAllFiles(f.getPath()));
                }
            }
        }
        return files;
    }

    /**
     * @param dirPath
     * @return
     * @describe 获取指定目录下的所有文件(不包括子文件夹)
     */
    public static ArrayList<File> getDirFiles(String dirPath) {
        File path = new File(dirPath);
        File[] fileArr = path.listFiles();
        ArrayList<File> files = new ArrayList<File>();
        for (File f : fileArr) {
            if (f.isFile()) {
                files.add(f);
            }
        }
        return files;
    }

    /**
     * @param dirPath 目录路径
     * @param suffix  文件后缀
     * @return
     * @describe 获取指定目录下特定文件后缀名的文件列表(不包括子文件夹)
     */
    public static ArrayList<File> getDirFiles(String dirPath, final String suffix) {
        File path = new File(dirPath);
        File[] fileArr = path.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                String lowerName = name.toLowerCase();
                String lowerSuffix = suffix.toLowerCase();
                if (lowerName.endsWith(lowerSuffix)) {
                    return true;
                }
                return false;
            }
        });
        ArrayList<File> files = new ArrayList<File>();
        for (File f : fileArr) {
            if (f.isFile()) {
                files.add(f);
            }
        }
        return files;
    }

    /**
     * @param fileName 待读取的完整文件名
     * @return 文件内容
     * @throws IOException
     * @describe 读取文件内容
     */
    public static String read(String fileName) throws IOException {
        File f = new File(fileName);
        FileInputStream fs = new FileInputStream(f);
        String result = null;
        byte[] b = new byte[fs.available()];
        fs.read(b);
        fs.close();
        result = new String(b);
        return result;
    }

    /**
     * @param fileName    目标文件名
     * @param fileContent 写入的内容
     * @return
     * @throws IOException
     * @describe 写文件
     */
    public static boolean write(String fileName, String fileContent) throws IOException {
        boolean result = false;
        File f = new File(fileName);
        FileOutputStream fs = new FileOutputStream(f);
        byte[] b = fileContent.getBytes();
        fs.write(b);
        fs.flush();
        fs.close();
        result = true;
        return result;
    }

    /**
     * @param fileName
     * @param fileContent
     * @return
     * @throws IOException
     * @describe 追加内容到指定文件
     */
    public static boolean append(String fileName, String fileContent) throws IOException {
        boolean result = false;
        File f = new File(fileName);
        if (f.exists()) {
            RandomAccessFile rFile = new RandomAccessFile(f, "rw");
            byte[] b = fileContent.getBytes();
            long originLen = f.length();
            rFile.setLength(originLen + b.length);
            rFile.seek(originLen);
            rFile.write(b);
            rFile.close();
        }
        result = true;
        return result;
    }

    /**
     * 拆分文件
     *
     * @param fileName 待拆分的完整文件名
     * @param byteSize 按多少字节大小拆分
     * @return 拆分后的文件名列表
     * @throws IOException
     */
    public List<String> splitBySize(String fileName, int byteSize) throws IOException {
        List<String> parts = new ArrayList<String>();
        File file = new File(fileName);
        int count = (int) Math.ceil(file.length() / (double) byteSize);
        int countLen = (count + "").length();
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(count, count * 3, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(count * 2));
        for (int i = 0; i < count; i++) {
            String partFileName = file.getName() + "." + leftPad((i + 1) + "", countLen, '0') + ".part";
            threadPool.execute(new SplitRunnable(byteSize, i * byteSize, SystemTypeCode.ROOT_PATH + partFileName, file));
            parts.add(partFileName);
            countThread++;
        }
        return parts;
    }

    /**
     * @param dirPath        合并文件所在目录名
     * @param partFileSuffix 拆分文件后缀名
     * @param partFileSize   拆分文件的字节数大小
     * @param mergeFileName  合并后的文件名
     * @throws IOException
     * @describe 合并文件
     */
    public void mergePartFiles(String dirPath, String partFileSuffix, int partFileSize, String mergeFileName) throws IOException {
        ArrayList<File> partFiles = FileUtil.getDirFiles(dirPath, partFileSuffix);
        Collections.sort(partFiles, new FileComparator());
        RandomAccessFile randomAccessFile = new RandomAccessFile(mergeFileName, "rw");
        randomAccessFile.setLength(partFileSize * (partFiles.size() - 1) + partFiles.get(partFiles.size() - 1).length());
        randomAccessFile.close();
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(partFiles.size(), partFiles.size() * 3, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(partFiles.size() * 2));
        for (int i = 0; i < partFiles.size(); i++) {
            threadPool.execute(new MergeRunnable(i * partFileSize, mergeFileName, partFiles.get(i)));
        }
    }

    /**
     * @describe 根据文件名，比较文件
     */
    private class FileComparator implements Comparator<File> {
        public int compare(File o1, File o2) {
            return o1.getName().compareToIgnoreCase(o2.getName());
        }
    }

    /**
     * @describe 分割处理Runnable
     */
    private class SplitRunnable implements Runnable {
        int byteSize;
        String partFileName;
        File originFile;
        int startPos;

        public SplitRunnable(int byteSize, int startPos, String partFileName, File originFile) {
            this.startPos = startPos;
            this.byteSize = byteSize;
            this.partFileName = partFileName;
            this.originFile = originFile;
        }

        public void run() {
            RandomAccessFile rFile;
            OutputStream os;
            try {
                rFile = new RandomAccessFile(originFile, "r");
                byte[] b = new byte[byteSize];
                rFile.seek(startPos);// 移动指针到每“段”开头
                int s = rFile.read(b);
                os = new FileOutputStream(partFileName);
                os.write(b, 0, s);
                os.flush();
                os.close();
                rFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                countThread--;
            }
        }
    }

    /**
     * @describe 合并处理Runnable
     */
    private class MergeRunnable implements Runnable {
        long startPos;
        String mergeFileName;
        File partFile;

        public MergeRunnable(long startPos, String mergeFileName, File partFile) {
            this.startPos = startPos;
            this.mergeFileName = mergeFileName;
            this.partFile = partFile;
        }

        public void run() {
            RandomAccessFile rFile;
            try {
                rFile = new RandomAccessFile(mergeFileName, "rw");
                rFile.seek(startPos);
                FileInputStream fs = new FileInputStream(partFile);
                byte[] b = new byte[fs.available()];
                fs.read(b);
                fs.close();
                rFile.write(b);
                rFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //    @Test
    public void writeFile() throws IOException, InterruptedException {
//        System.out.println(FileUtil.currentWorkDir);
//        StringBuilder sb = new StringBuilder();
//        long originFileSize = 1024 * 1024 * 100;// 100M

//        // 生成一个大文件
//        for (int i = 0; i < originFileSize; i++) {
//            sb.append("A");
//        }
//        String fileName = FileUtil.currentWorkDir + "origin.myfile";
//        System.out.println(fileName);
//        System.out.println(FileUtil.write(fileName, sb.toString()));
//        // 追加内容
//        sb.setLength(0);
//        sb.append("0123456789");
//        FileUtil.append(fileName, sb.toString());
//        long time = System.currentTimeMillis();
//        String fileName = "C:\\20170909142020-20170910142020.csv";
//        FileUtil fileUtil = new FileUtil();
//        fileUtil.splitBySize(fileName, blockFileSize);
//        long time2 = System.currentTimeMillis();
//        System.out.println(time2 - time);
//        Thread.sleep(10000);// 稍等10秒，等前面的小文件全都写完
        // 合并成新文件
//        fileUtil.mergePartFiles(FileUtil.currentWorkDir, ".part",blockFileSize, FileUtil.currentWorkDir + "20170909142020-20170910142020.csv");
    }

    public static float getfileSize(String filepath) {
        FileChannel fc = null;
        try {
            File f = new File(filepath);
            if (f.exists() && f.isFile()) {
                FileInputStream fis = new FileInputStream(f);
                fc = fis.getChannel();
                logger.info(String.valueOf(fc.size()));
                return fc.size();
            } else {
                logger.info("file doesn't exist or is not a file");
            }
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            if (null != fc) {
                try {
                    fc.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }
        }
        return Float.parseFloat("");
    }

    /**
     * 格式化单位：B|K|M|G
     *
     * @param fileS
     * @return
     */
    public static String FormetFileSize(long fileS) {//转换文件大小
        DecimalFormat df = new DecimalFormat("#.00");
        String fileSizeString = "";
        if (fileS < 1024) {
            fileSizeString = df.format((double) fileS) + "B";
        } else if (fileS < 1048576) {
            fileSizeString = df.format((double) fileS / 1024) + "K";
        } else if (fileS < 1073741824) {
            fileSizeString = df.format((double) fileS / 1048576) + "M";
        } else {
            fileSizeString = df.format((double) fileS / 1073741824) + "G";
        }
        return fileSizeString;
    }

    /*
      * 通过递归得到某一路径下所有的目录及其文件
      */
    public static List getFiles(String filePath) {
        File root = null;
        try {
            root = new File(URLDecoder.decode(filePath, "utf-8"));//解决中文乱麻问题
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        File[] files = root.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                getFiles(file.getAbsolutePath());
//                filelist.add(file.getAbsolutePath());
//                System.out.println("显示" + filePath + "下所有子目录及其文件" + file.getAbsolutePath());
            } else {
//                System.out.println("显示" + filePath + "下所有子目录" + file.getAbsolutePath());
                list.add(file.getAbsolutePath());
            }
        }
        return list;
    }

    /**
     * 字符流缓冲区读写文件
     * 效率比较高
     *
     * @param src 源文件
     * @param out 目标文件
     */
    public static void BufferReaderBufferWriter(String src, String out) {
        BufferedWriter bufferedWriter = null;
        BufferedReader bufferedReader = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(out));
            bufferedReader = new BufferedReader(new FileReader(src));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line);
                bufferedWriter.newLine();
                bufferedWriter.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.close();
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 对于文件大的文件更高效
     *
     * @param source
     * @param target
     * @throws IOException
     */
    public static void nioTransferCopy(File source, File target) throws IOException {
        if (source.isDirectory()) {
            File newFolder = new File(target, source.getName());
            newFolder.mkdirs();
            File[] fileArray = source.listFiles();
            for (File file : fileArray) {
                nioTransferCopy(file, newFolder);
            }
        } else {
            File newFolder = new File(target, source.getName());
            FileChannel in = null;
            FileChannel out = null;
            FileInputStream inStream = null;
            FileOutputStream outStream = null;
            try {
                inStream = new FileInputStream(source);
                if (target.isDirectory()) {//如果是目录就copy目录
                    outStream = new FileOutputStream(newFolder);
                } else {//copy文件
                    outStream = new FileOutputStream(target);
                }
                in = inStream.getChannel();
                out = outStream.getChannel();
                in.transferTo(0, in.size(), out);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                inStream.close();
                in.close();
                outStream.close();
                out.close();
            }
        }
    }

    public static void main(String[] args) {
//        File file = new File("E:\\lifelineapps\\lifeline_dubbo_qc_phoenix\\tmp\\orginal");
//        System.out.println(getFiles("E:\\lifelineapps\\lifeline_dubbo_qc_phoenix\\tmp\\orginal").get(0));
//        long time = System.currentTimeMillis();
//        String fileName = "C:\\20170909142020-20170910142020.csv";
//        FileUtil fileUtil = new FileUtil();
//        try {
//            fileUtil.splitBySize(fileName, blockFileSize);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        long time2 = System.currentTimeMillis();
//        System.out.println(time2 - time);
//        Thread.sleep(10000);// 稍等10秒，等前面的小文件全都写完
        long time0 = System.currentTimeMillis();
        String path = "E:\\lifelineapps\\lifeline_dubbo_qc_phoenix\\tmp\\orginal\\gas";
//        String outpath0="E:\\1.csv";
        String outpath1 = "E:\\";
//        BufferReaderBufferWriter(path,outpath0);
        long time1 = System.currentTimeMillis();
        System.out.println("第一种开销：" + (time1 - time0) / 1000 + "s");
        long time3 = System.currentTimeMillis();
        try {
            nioTransferCopy(new File(path), new File(outpath1));
        } catch (IOException e) {
            e.printStackTrace();
        }
        long time4 = System.currentTimeMillis();
        System.out.println("第二种开销：" + (time4 - time3) / 1000 + "s");
    }
}
