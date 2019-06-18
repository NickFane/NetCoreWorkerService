using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace sqs_processor
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IAmazonSQS _sqs;

        private readonly string _exampleQueueUrl = "https://sqs.ap-southeast-2.amazonaws.com/637294848563/example-queue";
        private readonly string _processedMessageQueueUrl = "https://sqs.ap-southeast-2.amazonaws.com/637294848563/processed-messages";

        public Worker(ILogger<Worker> logger, IAmazonSQS sqs)
        {
            _logger = logger;
            _sqs = sqs;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var request = new ReceiveMessageRequest
                    {
                        QueueUrl = _exampleQueueUrl,
                        MaxNumberOfMessages = 10,

                        WaitTimeSeconds = 5
                    };

                    var result = await _sqs.ReceiveMessageAsync(request);
                    if (result.Messages.Any())
                    {
                        foreach (var message in result.Messages)
                        {
                            // Some Processing code would live here
                            _logger.LogInformation("Processing Message: {message} | {time}", message.Body, DateTimeOffset.Now);

                            var processedMessage = new ProcessedMessage(message.Body);

                            var sendRequest = new SendMessageRequest(_processedMessageQueueUrl, JsonConvert.SerializeObject(processedMessage));

                            var sendResult = await _sqs.SendMessageAsync(sendRequest, stoppingToken);
                            if (sendResult.HttpStatusCode == System.Net.HttpStatusCode.OK)
                            {
                                var deleteResult = await _sqs.DeleteMessageAsync(_exampleQueueUrl, message.ReceiptHandle);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e.InnerException.ToString());
                }

                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            }
        }
    }

    public class ProcessedMessage
    {
        public ProcessedMessage(string message, bool hasErrors = false)
        {
            TimeStamp = DateTime.UtcNow;

            Message = message;
            HasErrors = hasErrors;
        }

        public DateTime TimeStamp { get; set; }
        public string Message { get; set; }
        public bool HasErrors { get; set; }
    }
}
