using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sqs_processor.Services
{
    public class QueueService : IQueueService
    {
        private readonly IAmazonSQS _sqsService;

        public QueueService()
        {
            _sqsService = new AmazonSQSClient();
        }

        public async Task<IEnumerable<string>> GetNextMessagesAsync()
        {
            var receiveMessageRequest = new ReceiveMessageRequest("example-queue");
            var message = await _sqsService.ReceiveMessageAsync(receiveMessageRequest);

            return message.Messages.Select(s => s.Body);
        }
    }

    public interface IQueueService
    {
        public Task<IEnumerable<string>> GetNextMessagesAsync();
    }
}
