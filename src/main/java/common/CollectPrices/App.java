package common.CollectPrices;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.iontrading.mkv.Mkv;
import com.iontrading.mkv.exceptions.MkvException;


public class App {
	
	protected ApplicationContext ac = null;
	
	protected Logger logger = null;
	
	public static void main(String[] args) {
		try {
			new App(args);
		} catch (Throwable t) {
			t.printStackTrace();
			Mkv.stop();
			System.exit(-1);
		}
	}
	
	protected App(String[] springApplicationContextFiles) throws MkvException {
        System.out.println( "Starting...." );
		
		System.out.println("************************");
		System.out.println("************************");
		System.out.println("************************");
		System.out.println("************************");

		ac = new ClassPathXmlApplicationContext(new String[] { "cfg/gov/beans.xml" });

		logger = LogManager.getLogger();

		logger.info("");
		logger.info("JVM arguments: " + ManagementFactory.getRuntimeMXBean().getInputArguments());

		logger.info("");
		logger.info("Program arguments: " + Arrays.toString(springApplicationContextFiles));

		logger.info("");
    }
	
}
