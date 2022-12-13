package org.example;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    // 获取Logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);

    // 定义两个属性，前后缀
    private String prefix;
    private String suffix;

    /**
     * 1.获取 Channel
     * 2.从 Channel获取事务和数据
     * 3.发送数据
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        // 1.定义返回值
        Status status = null;
        // 2.获取 Channel
        Channel channel = getChannel();
        // 3.从Channel获取事务
        Transaction transaction = channel.getTransaction();
        // 4.开启事务
        transaction.begin();

        try {
            // 5.从Channel获取数据
            Event event = channel.take();
            // 6.处理事件
            if (event != null) {
                String body = new String(event.getBody());
                logger.info(prefix+body+suffix);
            }
            // 7.提交事务
            transaction.commit();
            // 8.成功提交，修改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            // 9.提交事务失败
            transaction.rollback();
            // 10.修改状态
            status = Status.BACKOFF;
        } finally {
            // 11.最终关闭事务
            if (transaction != null) {
                transaction.close();
            }
        }
        // 12.返回状态信息
        return status;
    }

    @Override
    public void configure(Context context) {
        // 读取配置文件，为前后缀赋值
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "neu");
    }
}
