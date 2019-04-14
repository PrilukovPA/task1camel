package task1camel;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.RecipientList;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.main.Main;

/**
 * Класс реализует передачу сообщения из одной очереди Apache ActiveMQ в две другие, в каждую по одной копии. 
 * Добавлена обработка ошибок при получении пустого сообщения в виде вывода произвольного текста ошибки в консоль Java. 
 * Пустым сообщением считается сообщение, не содержащее ни одного символа.
 * @author Прилуков П.А.
 *
 */
public class Task1Camel {

	private Main main;
	
	public static void main(String[] args) throws Exception {
		Task1Camel task = new Task1Camel();
		task.execute();
	}
	
	public void execute() throws Exception {
		main = new Main();		
		main.addRouteBuilder(createRoute());		
		main.bind("jms", JmsComponent.jmsComponentAutoAcknowledge(new ActiveMQConnectionFactory()));
		System.out.println("Press Ctrl+C to terminate JVM\n");
        main.run();
	}
	
	/**
	 * Маршрут сообщений из очереди source.queue 
	 * в две другие очереди: target1.queue и target2.queue
	 * @return 
	 *
	 */
	private RouteBuilder createRoute() {
		return new RouteBuilder() {
			public void configure() throws Exception {

				onException(EmptyMessageException.class)
					.handled(true)
					.bean(new ExceptionHandler());				
			
				from("jms:source.queue")
					.routeId("ForkRoute")
					.process(new EmptyChecker())				
					.bean(RecipientListBean.class);
			}
		};
	}	
	
	public class ExceptionHandler {
		public void printErrorText(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Список точек назначения
	 *
	 */
	public static class RecipientListBean {
	    @RecipientList
	    public String[] route() {	        
	    	return new String[] {"jms:target1.queue", "jms:target2.queue"};	        
	    }
	}
	
	/**
	 * Класс реализует обрабоку сообщения, передаваемого по маршруту
	 *
	 */
	private class EmptyChecker implements Processor {
		public void process(Exchange exchange) throws Exception {
			Message msg = exchange.getIn();
			String txt = msg.getBody(String.class);
			if (txt == null || txt.isEmpty()) {
				throw new EmptyMessageException("Message is empty!");
			}
		}
	}
	
	/**
	 * Класс реализует исключение "пустое сообщение"
	 *
	 */
	@SuppressWarnings("serial")
	private class EmptyMessageException extends Exception {
		public EmptyMessageException(String responseString) {
			super(responseString);
		}
	}
}
