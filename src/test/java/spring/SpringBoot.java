package spring;/*
    user ji
    data 2019/3/9
    time 1:09 PM
    使用Spring boot的方式实现注入
*/
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

@SpringBootApplication
public class SpringBoot implements CommandLineRunner {
    @Autowired
    FsShell fsShell;

    @Override
    public void run(String... strings) throws Exception {
        System.out.println(fsShell.lsr("/").size());
    }

    public static void main(String[] args) {

        SpringApplication.run(SpringBoot.class,args);

    }
}
