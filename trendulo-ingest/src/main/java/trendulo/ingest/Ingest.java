package trendulo.ingest;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Ingest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		ApplicationContext context = new ClassPathXmlApplicationContext( "applicationContext.xml" );
		Hello hello = (Hello) context.getBean( "hello" );
		hello.sayHello();
	}

}
