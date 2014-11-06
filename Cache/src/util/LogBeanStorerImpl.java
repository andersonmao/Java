package util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @author Anderson Mao, 2014-11-05
 */
public class LogBeanStorerImpl implements LogBeanStorer{
	private final static Logger logger = LoggerFactory.getLogger(LogBeanStorerImpl.class);
	
	private static final int QUEUE_MAX_CAPACITY = 30000;
	private static final int QUEUE_STEP         = 1000;
	private static final int QUEUE_THREAD_COUNT = 2;
	private static final int RETRY_SLEEP_SECOND = 20;
	
	private static AtomicLong totalOffer = new AtomicLong();
	private static AtomicLong totalSent = new AtomicLong();
	private static LinkedBlockingQueue<LogBean> queue = null;
	
	@Autowired
	private MongoTemplate mongoTemplate;
	
	@Override
	public void insert(LogBean logBean){
		boolean re = false;
		try{
			re = queue.offer(logBean, 1, TimeUnit.MINUTES);
		}catch(InterruptedException ex){
			// Ignore
		}
		if(!re){
			logger.error("queue is full, skip insert: queueSize="+queue.size() );
			return;
		}
		long offerCount = totalOffer.incrementAndGet();
		int size = queue.size();
		if(offerCount % QUEUE_STEP == 0){
			logger.debug("insert: queueSize="+size+", totalOffer="+offerCount);
		}
	}	
	
	protected void init(){
		synchronized(LogBeanStorerImpl.class){
			if(queue != null){
				return;
			}
			logger.info("init: threadCount="+QUEUE_THREAD_COUNT+", queueCapacity="+QUEUE_MAX_CAPACITY);
			queue = new LinkedBlockingQueue<LogBean>(QUEUE_MAX_CAPACITY);
			ExecutorService exec = Executors.newFixedThreadPool(QUEUE_THREAD_COUNT);
			for(int i=0; i< QUEUE_THREAD_COUNT; i++){
				QueueStorer qs = new QueueStorer(mongoTemplate);
				exec.submit(qs);
			}
			exec.shutdown();
		}
	}
	
	private static class QueueStorer implements Callable<Long>{
		private MongoTemplate mongoTemplate = null;
		
		public QueueStorer(MongoTemplate mongoTemplate){
			this.mongoTemplate = mongoTemplate;
		}
		
		@Override
		public Long call() throws Exception {
			if(mongoTemplate == null){
				logger.error("mongoTemplate is invalid null");
				return -1L;
			}
			logger.info("begin: thread="+Thread.currentThread().getId() );
			boolean flag = true;
			LogBean logBean = null;
			while(flag){
				try{
					if(logBean == null){
						logBean = queue.take();
					}else{
						// If exception during mongoTemplate.insert(), will sleep and retry for same logBean
						// So the MongoDB can stop and restart, application will not run into exception and will not lose logBean
						Thread.sleep(RETRY_SLEEP_SECOND * 1000);
					}
					int size = queue.size();
					mongoTemplate.insert(logBean);
					logBean = null;
					long sent = totalSent.incrementAndGet();
					if(sent % QUEUE_STEP == 0){
						logger.debug("QueueStorer: thread="+Thread.currentThread().getId()+", queueSize="+size+", totalSent="+sent+", totalOffer="+totalOffer.longValue() );
					}
				}catch(InterruptedException ex){
					logger.info("return when InterruptedException");
					return -1L;
				}catch(Exception ex){
					// Do NOT set logBean = null when exception
					logger.error("sleep "+RETRY_SLEEP_SECOND+" seconds after error in call: "+ex.getMessage() );
				}
			}
			return 0L;
		}
	}
}
