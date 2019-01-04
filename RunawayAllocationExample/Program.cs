using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confluent_Kafka_Core___Simple_Consumer
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			var config = new List<KeyValuePair<string, string>>
			{
				new KeyValuePair<string, string>("bootstrap.servers", "YOURSERVERSHERE:9092"),
				new KeyValuePair<string, string>("group.id", "YOURGROUPIDHERE"),
				new KeyValuePair<string, string>("auto.offset.reset", "earliest"),
				new KeyValuePair<string, string>("statistics.interval.ms", "10000"),
				//new KeyValuePair<string, string>("queued.min.messages", "1"),
				//new KeyValuePair<string, string>("queued.max.messages.kbytes", "100")
			};

			ConsumeRecords(config);

			Console.WriteLine(string.Empty);
			Console.WriteLine("\nEnd of line.");
			Console.ReadLine();
		}

		private static void ConsumeRecords(List<KeyValuePair<string, string>> config)
		{
			var consumer = new Consumer<string, string>(config);
			consumer.Subscribe("YOURTOPICHERE");

			consumer.OnStatistics += Consumer_OnStatistics;

			using (consumer)
			{
				int totalConsumedMessages = 0;
				for (int i = 0; i < 2000; i++)
				{
					var result = Poll(consumer);
					totalConsumedMessages += result.Count;

					if (totalConsumedMessages % 1000 == 0)
					{
						Console.Write($"\rConsumed {totalConsumedMessages} msgs");
					}
				}
			}
		}

		private static void Consumer_OnStatistics(object sender, string e)
		{
			Console.WriteLine($"{e}");
		}

		public static List<ConsumeResult<string, string>> Poll(Consumer<string, string> confluentConsumer)
		{
			var kafkaMessageList = new List<ConsumeResult<string, string>>();
			int messageCounter = 0;

			while (messageCounter < 1000)
			{
				var consumeResult = confluentConsumer.Consume();
				kafkaMessageList.Add(consumeResult);
				Task.Delay(1).Wait();
				messageCounter++;
			}

			return kafkaMessageList;
		}
	}
}
