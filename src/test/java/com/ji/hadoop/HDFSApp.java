package com.ji.hadoop;/*
    user ji
    data 2019/3/7
    time 4:55 PM
    hadoop java api 操作
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import javax.rmi.CORBA.Util;
import java.io.*;
import java.net.URI;
import java.text.SimpleDateFormat;

public class HDFSApp {
    FileSystem fileSystem = null;
    Configuration configuration = null;
    public static String HDFS_PATH = "hdfs://172.31.42.24:8020";
    /*
    在每个方法执行前执行这个方法；
     */
    @Before
    public void setUp() throws Exception {
        System.out.println("start");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
    }
    /*
    在每个方法执行后执行这个方法；
     */
    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("all done ,finished");
    }

    /*
    创建文件夹
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }
    /**
     * 判断路径是否存在
     */
    @Test
    public static boolean test(Configuration conf, String path) {
        try (FileSystem fs = FileSystem.get(conf)) {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 复制文件到指定路径 若路径已存在，则进行覆盖
     */
    public static void copyFromLocalFile(Configuration conf,
                                         String localFilePath, String remoteFilePath) {
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf)) {
            /* fs.copyFromLocalFile 第一个参数表示是否删除源文件，第二个参数表示是否覆盖 */
            fs.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    /*
    创建文件
     */
    @Test
    public void createFile() throws Exception {
        FSDataOutputStream fdos = fileSystem.create(new Path("/hdfsapi/test/hello1.txt"));
        fdos.write("hello,this is world ".getBytes());
        fdos.write("this is my name".getBytes());
        fdos.flush();
        fdos.close();
    }

    /*
    查看文件内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream input = fileSystem.open(new Path("/hdfsapi/test/hello1.txt"));
        IOUtils.copyBytes(input, System.out, 1024);
        input.close();
    }
    /**
     * 追加文件内容
     */
    @Test
    public static void appendToFile(Configuration conf, String localFilePath,
                                    String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf);
             FileInputStream in = new FileInputStream(localFilePath);) {
            FSDataOutputStream out = fs.append(remotePath);
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = in.read(data)) > 0) {
                out.write(data, 0, read);
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*
    rename 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/hello1.txt");
        Path newPath = new Path("/hdfsapi/test/hello2.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /*
    copyFromLocal
     */
    @Test
    public void copyFromLocal() throws Exception {
        Path localPath = new Path("/Users/ji/Downloads/hadoop-2.6.0-cdh5.7.0/h.txt");
        Path remotePath = new Path("/local/h.txt");
        fileSystem.copyFromLocalFile(localPath, remotePath);
    }

    /*
   copyFromLocalWithProgress
    */
    @Test
    public void copyFromLocalWithProgress() throws Exception {
        InputStream input = new BufferedInputStream(new FileInputStream(new File("/Users/ji/Downloads/hadoop-2.6.0-cdh5.7.0.tar")));
        FSDataOutputStream output = fileSystem.create(new Path("/local/hadoop.tar"), new Progressable() {
            @Override
            public void progress() {
                System.out.print("--");//带进度条
            }
        });
        IOUtils.copyBytes(input, output, 4096);
    }

    /*
    download file
     */
    @Test
    public void downLoad() throws Exception {
        Path localPath = new Path("/hello.txt");
        Path remotePath = new Path("/Users/ji/Downloads/hadoop-2.6.0-cdh5.7.0/remote.txt");
        fileSystem.copyToLocalFile(localPath, remotePath);
    }

    /*
    开始展示所有的文件
    listFile
     */
    @Test
    public void listFile() throws Exception {
        FileStatus[] fs = fileSystem.listStatus(new Path("/"));
        for (FileStatus file:fs
             ) {
            System.out.println(file.getPath()+"  "+file.getReplication());
        }
    }
    /**
     * 下载文件到本地 判断本地路径是否已存在，若已存在，则自动进行重命名
     */
    @Test
    public static void copyToLocal(Configuration conf, String remoteFilePath,
                                   String localFilePath) {
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf)) {
            File f = new File(localFilePath);
            /* 如果文件名存在，自动重命名(在文件名后面加上 _0, _1 ...) */
            if (f.exists()) {
                System.out.println(localFilePath + " 已存在.");
                Integer i = Integer.valueOf(0);
                while (true) {
                    f = new File(localFilePath + "_" + i.toString());
                    if (!f.exists()) {
                        localFilePath = localFilePath + "_" + i.toString();
                        break;
                    } else {
                        i++;
                        continue;
                    }
                }
                System.out.println("将重新命名为: " + localFilePath);
            }
            // 下载文件到本地
            Path localPath = new Path(localFilePath);
            fs.copyToLocalFile(remotePath, localPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /**
     * 显示指定文件的信息
     */
    @Test
    public static void ls(Configuration conf, String remoteFilePath) {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path remotePath = new Path(remoteFilePath);
            FileStatus[] fileStatuses = fs.listStatus(remotePath);
            for (FileStatus s : fileStatuses) {
                System.out.println("路径: " + s.getPath().toString());
                System.out.println("权限: " + s.getPermission().toString());
                System.out.println("大小: " + s.getLen());
                /* 返回的是时间戳,转化为时间日期格式 */
                long timeStamp = s.getModificationTime();
                SimpleDateFormat format = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                String date = format.format(timeStamp);
                System.out.println("时间: " + date);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 显示指定文件夹下所有文件的信息（递归）
     */
    @Test
    public static void lsDir(Configuration conf, String remoteDir) {
        try (FileSystem fs = FileSystem.get(conf)) {
            Path dirPath = new Path(remoteDir);
            /* 递归获取目录下的所有文件 */
            RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(
                    dirPath, true);
            /* 输出每个文件的信息 */
            while (remoteIterator.hasNext()) {
                FileStatus s = remoteIterator.next();
                System.out.println("路径: " + s.getPath().toString());
                System.out.println("权限: " + s.getPermission().toString());
                System.out.println("大小: " + s.getLen());
                /* 返回的是时间戳,转化为时间日期格式 */
                Long timeStamp = s.getModificationTime();
                SimpleDateFormat format = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss");
                String date = format.format(timeStamp);
                System.out.println("时间: " + date);
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*
    delete
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path(""));

    }
}
