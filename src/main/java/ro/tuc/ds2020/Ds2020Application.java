package ro.tuc.ds2020;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.io.*;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

@SpringBootApplication
public class Ds2020Application extends SpringBootServletInitializer {
    public static void main(String[] args) throws IOException, TimeoutException, URISyntaxException, NoSuchAlgorithmException, KeyManagementException, InterruptedException {
        System.out.println(args[0]);
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        System.out.println(123);
        String uri = "amqps://pxycwtcm:xMByUVCGay2FpwL0wvI5TkmC26FclX9b@goose.rmq2.cloudamqp.com/pxycwtcm";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(uri);
        factory.setConnectionTimeout(30000);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String queue = "SensorMeasurements";
        boolean durable = false;
        boolean exclusive = false;
        boolean autoDelete = false;

        channel.queueDeclare(queue, durable, exclusive, autoDelete, null);

        BufferedReader br = new BufferedReader(new FileReader(new File("sensor.csv")));
        String read = null;
        String[] arr;

        while ((read = br.readLine()) != null) {
            arr = read.split(",");
            for (String s : arr) {
                //System.out.println(s);
                LocalDateTime ldt = LocalDateTime.now();
                //System.out.println(ldt);
                //long timestamp = ldt.toEpochSecond(ZoneOffset.UTC);
                //System.out.println(timestamp);
                double energyValue = Double.parseDouble(s);
                //System.out.println(energyValue);
                String msg = "{" + "\"timestamp\"" + ":\"" + ldt + "\"," + "\"energyValue\"" + ":" + energyValue + ", " +
                        "\"device\"" + ":{" + "\"id\"" + ":" + args[0] + "}}";
                System.out.println(msg);
                channel.basicPublish("", "SensorMeasurements", null, msg.getBytes());
                Thread.sleep(1000);
            }
        }
        br.close();
        channel.close();
        connection.close();

        SpringApplication.run(Ds2020Application.class, args);
    }
}
