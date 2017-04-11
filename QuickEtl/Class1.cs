using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickEtl
{
    using System.Threading;
    using System.Threading.Tasks.Dataflow;

    /// <summary>
    /// For http://stackoverflow.com/questions/13510094/tpl-dataflow-guarantee-completion-only-when-all-source-data-blocks-completed
    /// </summary>
    public class Class1
    {
        private BroadcastBlock<int> broadCaster;
        private TransformBlock<int, string> transformBlock1;
        private TransformBlock<int, string> transformBlock2;
        private ActionBlock<string> processor;

        public Class1()
        {
            broadCaster = new BroadcastBlock<int>(
                i =>
                {
                    return i;
                });

            transformBlock1 = new TransformBlock<int, string>(
                i =>
                {
                    Console.WriteLine("1 input count: " + transformBlock1.InputCount);
                    Thread.Sleep(50);
                    return ("1_" + i);
                });

            transformBlock2 = new TransformBlock<int, string>(
                i =>
                {
                    Console.WriteLine("2 input count: " + transformBlock2.InputCount);
                    Thread.Sleep(20);
                    return ("2_" + i);
                });

            processor = new ActionBlock<string>(
                i =>
                {
                    Console.WriteLine(i);
                });

            broadCaster.LinkTo(transformBlock1, new DataflowLinkOptions {PropagateCompletion = true});
            broadCaster.LinkTo(transformBlock2, new DataflowLinkOptions {PropagateCompletion = true});
            transformBlock1.LinkTo(processor, new DataflowLinkOptions {PropagateCompletion = true});
            transformBlock2.LinkTo(processor, new DataflowLinkOptions {PropagateCompletion = true});
        }

        public void Start()
        {
            const int numElements = 100;

            for (int i = 1; i <= numElements; i++)
            {
                broadCaster.SendAsync(i);
            }

            //mark completion
            broadCaster.Complete();
            processor.Complete();
            broadCaster.Completion.Wait();
            processor.Completion.Wait();

            Console.WriteLine("Finished");
            Console.ReadLine();
        }
    }
}
