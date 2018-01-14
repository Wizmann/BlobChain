using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BlobChain;

namespace SmokeTest
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        static async Task MainAsync(string[] args)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse("{{ your connection string here }}");

            var chain = new AppendBlobChain(account, "appendblobchain", Guid.NewGuid().ToString());

            List<Task> tasks = new List<Task>();

            Console.WriteLine(DateTimeOffset.UtcNow);

            for (int i = 0; i < 30; i++)
            {
                tasks.Add(AppendMany(chain, 10000));
            }

            await Task.WhenAll(tasks);

            Console.WriteLine(DateTimeOffset.UtcNow);

            Console.WriteLine("---");

            Console.WriteLine(DateTimeOffset.UtcNow.ToString("o"));
            Console.WriteLine(await chain.TotalSize());
            Console.WriteLine(DateTimeOffset.UtcNow.ToString("o"));

            Console.WriteLine("---");

            Console.WriteLine(DateTimeOffset.UtcNow.ToString("o"));
            Console.WriteLine(await chain.TotalCommitCount());
            Console.WriteLine(DateTimeOffset.UtcNow.ToString("o"));

            Console.ReadLine();
        }

        static async Task AppendMany(AppendBlobChain chain, int count)
        {
            for (int i = 0; i < count; i++)
            {
                await chain.Append(" ");
            }
        }
    }
}
