
namespace Azure.Functions.Extensions.SQS
{
    using System.Reflection;
    using System.Threading.Tasks;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Host;
    using Microsoft.Azure.WebJobs.Host.Triggers;
    using Microsoft.Extensions.Options;

    public class SqsQueueTriggerBindingProvider : ITriggerBindingProvider
    {
        private IOptions<SqsQueueOptions> SqsQueueOptions { get; set; }

        private INameResolver NameResolver { get; set; }

        public SqsQueueTriggerBindingProvider(IOptions<SqsQueueOptions> sqsQueueOptions, INameResolver nameResolver)
        {
            SqsQueueOptions = sqsQueueOptions;
            NameResolver = nameResolver;
        }

        public Task<ITriggerBinding> TryCreateAsync(TriggerBindingProviderContext context)
        {
            var triggerAttribute = context.Parameter.GetCustomAttribute<SqsQueueTriggerAttribute>(inherit: false);
            return triggerAttribute is null
                ? Task.FromResult<ITriggerBinding>(null)
                : Task.FromResult<ITriggerBinding>(new SqsQueueTriggerBinding(parameterInfo: context.Parameter, triggerParameters: ResolveTriggerParameters(triggerAttribute), sqsQueueOptions: SqsQueueOptions));
        }

        private SqsQueueTriggerAttribute ResolveTriggerParameters(SqsQueueTriggerAttribute triggerAttribute)
        {
            return new SqsQueueTriggerAttribute
            {
                AWSKeyId = Resolve(triggerAttribute.AWSKeyId),
                AWSAccessKey = Resolve(triggerAttribute.AWSAccessKey),
                QueueUrl = Resolve(triggerAttribute.QueueUrl),
                ExponentialRetry = Resolve(triggerAttribute.ExponentialRetry),
                BaseBackOff = Resolve(triggerAttribute.BaseBackOff),
                MaxBackOff = Resolve(triggerAttribute.MaxBackOff)
            };
        }

        private string Resolve(string property)
        {
            return NameResolver.Resolve(property) ?? NameResolver.ResolveWholeString(property) ?? property;
        }
    }
}
