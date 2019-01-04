package com.trimble.vss;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.System.out;

public class Main {

	public static void main(String[] args)
	{
		try {
			Properties producerConfig = new Properties();
			producerConfig.put("bootstrap.servers", "YOURSERVERHERE:9092");
			producerConfig.put("acks", "1");
			producerConfig.put("compression.type", "lz4");
			producerConfig.put("max.block.ms", "120000");
			producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			producerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			producerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			KafkaProducer producer = new KafkaProducer(producerConfig);

			out.println("About to produce records...");
			ArrayList<Future> sendResults = new ArrayList<Future>();
			for (int i = 0; i < 1000000; i++)
			{
				String msgKey = UUID.randomUUID().toString();
				String serializedXml = String.format("<?xml version=\"1.0\" encoding=\"utf-16\"?><UserMessage xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">  <ID>%s</ID>  <FirstName>FirstName%s</FirstName>  <MiddleName>MiddleName3%s</MiddleName>  <LastName>LastName%s</LastName>  <StreetAddress1>StreetAddress%s</StreetAddress1>  <StreetAddress2>StreetAddress%s</StreetAddress2>  <City>City%s</City>  <State>State%s</State>  <ZipCode>ZipCode%s</ZipCode>  <County>County%s</County>  <Country>Country%s</Country>  <PhoneNumber>PhoneNumber%s</PhoneNumber>  <EmailAddress>EmailAddress%s</EmailAddress>  <SocialSecurityNumber>SocialSecurityNumber%s</SocialSecurityNumber>  <LicensePlateNumber>LicensePlateNumber%s</LicensePlateNumber>  <BloodType>BloodType%s</BloodType></UserMessage>", msgKey, UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());

				ProducerRecord msgToSend = new ProducerRecord("YOURTOPICHERE", msgKey, serializedXml);
				Future futureFromSend = producer.send(msgToSend,
					new Callback() {
						public void onCompletion(RecordMetadata metadata, Exception e) {
							if(e != null) {
								e.printStackTrace();
							}
						}
					});

				if (futureFromSend != null)
				{
					sendResults.add(futureFromSend);
				}

				if (i % 1000 == 0)
				{
					out.printf("\rproduced %d records so far", i);
					producer.flush();
				}
			}

			out.print("\rVerifying messages sent...");
			for(Future sendResult : sendResults)
			{
				//block send()'s future returns
				sendResult.get();
			}

			producer.close();

			out.println("End of line - hit enter to terminate");
			System.in.read();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
}


