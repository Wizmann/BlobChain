using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BlobChain
{
    public class AppendBlobChain
    {
        public enum ErrorCode
        {
            Unknown = 0,
            OK = 200,
            Conflict = 419
        }

        public struct BlobChainOffset
        {
            public int BlobSuffix { get; set; }

            public long DataOffset { get; set; }

            public ErrorCode StatusCode { get; set; }
        }

        public static readonly int MaxAppendBlobCommittedBlockCount = 50000;

        private CloudStorageAccount StorageAccount { get; set; }

        private string BlobContainerName { get; set; }

        private string AppendBlobName { get; set; }

        private int blobSuffix;

        private string GetBlobNameWithSuffix(int blobSuffix)
        {
            return $"{this.AppendBlobName}.{blobSuffix}";
        }

        private CloudBlobContainer Conatiner { get; set; }

        public AppendBlobChain(CloudStorageAccount storageAccount, string blobContainerName, string appendBlobName)
        {
            this.StorageAccount = storageAccount;
            this.BlobContainerName = blobContainerName;
            this.AppendBlobName = appendBlobName;

            this.blobSuffix = -1;

            var client = this.StorageAccount.CreateCloudBlobClient();
            this.Conatiner = client.GetContainerReference(this.BlobContainerName);
            this.Conatiner.CreateIfNotExists();
        }

        public async Task<BlobChainOffset> Append(byte[] bytes)
        {
            using (var ms = new MemoryStream(bytes))
            {
                return await this.Append(ms);
            }
        }

        public async Task<long> TotalSize()
        {
            long size = 0;
            int suffix = 0;
            while (true)
            {
                string blobName = this.GetBlobNameWithSuffix(suffix);
                var blob = this.Conatiner.GetAppendBlobReference(blobName);
                if (await blob.ExistsAsync())
                {
                    size += blob.Properties.Length;
                }
                else
                {
                    break;
                }
                suffix += 1;
            }
            return size;
        }

        public async Task<long> TotalCommitCount()
        {
            long size = 0;
            int suffix = 0;
            while (true)
            {
                string blobName = this.GetBlobNameWithSuffix(suffix);
                var blob = this.Conatiner.GetAppendBlobReference(blobName);
                if (await blob.ExistsAsync())
                {
                    size += blob.Properties.AppendBlobCommittedBlockCount.Value;
                }
                else
                {
                    break;
                }
                suffix += 1;
            }
            return size;
        }

        private async Task<BlobChainOffset> DoAppend(Stream stream)
        {
            long offset = -1;
            int blobSuffix = this.blobSuffix;
            string blobName = this.GetBlobNameWithSuffix(blobSuffix);
            var appendBlob = this.Conatiner.GetAppendBlobReference(blobName);

            /*
            await appendBlob.FetchAttributesAsync();
            if (appendBlob.Properties.AppendBlobCommittedBlockCount >= MaxAppendBlobCommittedBlockCount)
            {
                return new BlobChainOffset()
                {
                    StatusCode = ErrorCode.Conflict
                };
            }
            */

            stream.Position = 0;
            try
            {
                offset = await appendBlob.AppendBlockAsync(stream);
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == (int) HttpStatusCode.Conflict)
            {
                return new BlobChainOffset()
                {
                    StatusCode = ErrorCode.Conflict
                };
            }

            return new BlobChainOffset()
            {
                BlobSuffix = blobSuffix,
                DataOffset = offset,
                StatusCode = ErrorCode.OK
            };

        }

        private async Task<ErrorCode> CreateAppendBlobIfNotExists(string blobName)
        {
            try
            {
                var currentBlob = this.Conatiner.GetAppendBlobReference(blobName);
                await currentBlob.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), null, null);
            }
            catch (StorageException se) when (se.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.Conflict)
            {
                return ErrorCode.Conflict;
            }
            return ErrorCode.OK;
        }

        private async Task CreateNewBlobOnChain()
        {
            int curSuffix = this.blobSuffix;

            if (curSuffix >= 0)
            {
                var curBlob = this.Conatiner.GetAppendBlobReference(this.GetBlobNameWithSuffix(curSuffix));

                await curBlob.FetchAttributesAsync();
                Console.WriteLine(curBlob.Properties.AppendBlobCommittedBlockCount.Value);
                if (curBlob.Properties.AppendBlobCommittedBlockCount.Value < MaxAppendBlobCommittedBlockCount)
                {
                    return;
                }
            }

            int nextSuffix = curSuffix + 1;
            string nextBlobName = this.GetBlobNameWithSuffix(nextSuffix);

            var res = await this.CreateAppendBlobIfNotExists(nextBlobName);
            if (res == ErrorCode.OK || res == ErrorCode.Conflict)
            {
                this.blobSuffix = Math.Max(this.blobSuffix, nextSuffix);
            }
        }

        public async Task<BlobChainOffset> Append(Stream stream)
        {
            while (true)
            {
                var res = new BlobChainOffset();
                if (this.blobSuffix < 0)
                {
                    await this.CreateNewBlobOnChain();
                    continue;
                }
                else
                {
                    res = await DoAppend(stream);
                }

                if (res.StatusCode == ErrorCode.OK)
                {
                    return res;
                }
                else if (res.StatusCode == ErrorCode.Conflict)
                {
                    await this.CreateNewBlobOnChain();
                }
                else
                {
                    throw new InvalidOperationException($"invalid error code: {res.StatusCode}");
                }
            }
        }

        public async Task<BlobChainOffset> Append(string text, Func<string, byte[]> encoder = null)
        {
            byte[] bytes = null;
            if (encoder == null)
            {
                bytes = this.TextEncoder(text);
            }
            else
            {
                bytes = encoder(text);
            }
            return await this.Append(bytes);
        }

        private byte[] TextEncoder(string text)
        {
            var bytes = UTF8Encoding.UTF8.GetBytes(text);
            return bytes;
        }
    }
}
