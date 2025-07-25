﻿
using System.Linq;

namespace Azure.Functions.Extensions.SQS
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Threading.Tasks;
    using Amazon.SQS.Model;
    using Microsoft.Azure.WebJobs.Host.Bindings;
    using Microsoft.Azure.WebJobs.Host.Listeners;
    using Microsoft.Azure.WebJobs.Host.Protocols;
    using Microsoft.Azure.WebJobs.Host.Triggers;
    using Microsoft.Extensions.Options;

    public class SqsQueueTriggerBinding : ITriggerBinding
    {
        private IOptions<SqsQueueOptions> SqsQueueOptions { get; set; }

        private SqsQueueTriggerAttribute TriggerParameters { get; set; }

        private ParameterInfo ParameterInfo { get; set; }

        public Type TriggerValueType => typeof(Message);

        public IReadOnlyDictionary<string, Type> BindingDataContract { get; } = new Dictionary<string, Type>();

        public SqsQueueTriggerBinding(ParameterInfo parameterInfo, SqsQueueTriggerAttribute triggerParameters, IOptions<SqsQueueOptions> sqsQueueOptions)
        {
            this.SqsQueueOptions = sqsQueueOptions;
            this.ParameterInfo = parameterInfo;
            this.TriggerParameters = triggerParameters;
        }

        public Task<ITriggerData> BindAsync(object value, ValueBindingContext context)
        {
            var message = value as Message;
            return Task.FromResult<ITriggerData>(new TriggerData(
                valueProvider: new SqsQueueMessageValueProvider(message.Body),
                bindingData: message.Attributes.ToDictionary(x => x.Key.ToString(), x => x.Value as object)));
        }

        public Task<IListener> CreateListenerAsync(ListenerFactoryContext context)
        {
            return Task.FromResult<IListener>(new SqsQueueTriggerListener(
                triggerParameters: this.TriggerParameters,
                sqsQueueOptions: this.SqsQueueOptions,
                executor: context.Executor));
        }

        public ParameterDescriptor ToParameterDescriptor()
        {
            return new ParameterDescriptor
            {
                Name = this.ParameterInfo.Name
            };
        }
    }
}
