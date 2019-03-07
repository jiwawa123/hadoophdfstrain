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
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

public class HDFSApp {
    FileSystem fileSystem = null;
    Configuration configuration = null;
    public static String HDFS_PATH = "hdfs://localhost:8020";

    @Before
    public void setUp() throws Exception {
        System.out.println("start");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
    }

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
            System.out.println(file.getLen());
            System.out.println(file.getPath());
            System.out.println(file.isDirectory());
            System.out.println(file.getReplication());
        }
    }
}
