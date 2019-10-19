package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        //1.客户端获取
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop110:9000"),
                new Configuration(), "atguigu");
        boolean mkdirs = fileSystem.mkdirs(new Path("/ttttt/aaa/bbb"));
        if (mkdirs) {
            System.out.println("成功");
        } else {
            System.out.println("失败");
        }
        fileSystem.close();
    }

    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://hadoop110:9000"),
                new Configuration(),"atguigu");
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getModificationTime());
            if (fileStatus.isFile()) {
                long blockSize = fileStatus.getBlockSize();
                long size = fileStatus.getLen();
                short replication = fileStatus.getReplication();
                System.out.println(blockSize + "\t" + size + "\t" + replication);
            } else {
                System.out.println("文件夹");
            }
        }
    }

    @Test
    public void put() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        fs = FileSystem.get(URI.create("hdfs://hadoop110:9000"), configuration, "atguigu");
        fs.copyFromLocalFile(new Path("d:/1.txt"),new Path("/1.txt"));
    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/1.txt"));
        append.write("xiaxielsfasldfalflaflaflakflaflkeikkkakdao".getBytes());
        IOUtils.closeStream(append);
    }

    @Test
    public void cat() throws IOException {
        FSDataInputStream open = fs.open(new Path("/1.txt"));
        byte[] buf = new byte[1024];
        int len;
        while ((len = open.read(buf)) > 0) {
            String line = new String(buf, 0, len);
            System.out.println(line);
        }
        IOUtils.closeStream(open);
    }

    @Test
    public void change() throws IOException {
        fs.setPermission(new Path("/1.txt"),FsPermission.valueOf("-rwxrwxrwx"));
    }

    @Test
    public void chown() throws IOException {
        fs.setOwner(new Path("/1.txt"),"ttt","tttt");
    }

    @Test
    public void get() throws IOException {
        fs.copyToLocalFile(new Path("/1.txt"), new Path("d:/ttt.xtxt"));
    }

    @Test
    public void cp() throws IOException {
        FSDataInputStream open = fs.open(new Path("/1.txt"));
        FSDataOutputStream creat = fs.create(new Path("/5.txt"));
        IOUtils.copyBytes(open,creat,1024);
        IOUtils.closeStream(open);
        IOUtils.closeStream(creat);
    }

    @Test
    public void mv () throws IOException {
        boolean rename = fs.rename(new Path("/3.txt"), new Path("/ttttt/2.txt"));
        System.out.println(rename);
    }

    @Test
    public void rm () throws IOException {
        boolean delete = fs.delete(new Path("/ttttt/"), true);
        System.out.println(delete);
    }

    @Test
    public void setrep() throws IOException {
        fs.setReplication(new Path("/5.txt"), (short) 10);
    }

    @After
    public void after() throws IOException {
        fs.close();
    }

}