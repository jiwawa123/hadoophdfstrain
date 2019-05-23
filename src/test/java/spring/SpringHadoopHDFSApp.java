package spring;/*
    user ji
    data 2019/3/9
    time 11:59 AM
    通过添加spring ioc的方式，解决依赖注入问题
*/

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringHadoopHDFSApp {
    private ApplicationContext ac;
    private FileSystem fileSystem;

    @Before
    public void setup() {
        ac = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ac.getBean("fileSystem");
        System.out.println("start up");

    }

    @Test
    public void makedir() throws Exception {
        fileSystem.mkdirs(new Path("/springPro"));

    }

    @After
    public void shutdown() throws Exception {
        ac = null;
        fileSystem.close();
        System.out.println("shutdown");
    }
}
