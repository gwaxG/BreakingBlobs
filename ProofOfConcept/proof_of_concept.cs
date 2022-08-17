namespace ProofOfConcept;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Xunit;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Azure.Storage.Blobs.Models;


using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;


public class proof_of_concept : IDisposable
{
    private const string _connectionString =
        "DefaultEndpointsProtocol=https;AccountName=bigfilestest;AccountKey=YvclLEtAM0FvgBkgbTQV6Xch97GOMxgR2fanGm/WSIJkOfVZ7oTEWHwxeXegUBR39oKdSFqahpugGAx5dknpIQ==";

    private int _blockNum = 5;

    private int _repetitionNum = 50;

    private byte[] _buffer;

    public proof_of_concept()
    {
        _buffer = new byte[1024 * 1024 / 8];
    }

    public string Base64Encode(string plainText)
        {
            var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

    private async Task<(string?, DateTimeOffset?, List<string>)> GetAllBlockIdsAsync(CloudBlockBlob blob)
    {
        try
        {
            var blockItems = await blob.DownloadBlockListAsync();
            var etag = blob.Properties.ETag;
            var date = blob.Properties.LastModified;
            var blockIds = blockItems.Select(b => b.Name).ToList();
            return (etag, date, blockIds);
        }
        // Storage exception can be thrown if there is no any written blob.
        catch (StorageException e)
        {
            if (e.RequestInformation.HttpStatusCode == 404)
                return (null, null, new List<string>());
            else
                throw;
        }
    }

    private string GetNewBlockId()
    {
        return Base64Encode(Guid.NewGuid().ToString("N"));
    }

    [Fact]
    public async  void parallel_writing_workaround()
    {
        var client = CloudStorageAccount.Parse(_connectionString).CreateCloudBlobClient();
        var logBlobContainerName = "dev-" + Guid.NewGuid().ToString();
        var container = client.GetContainerReference(logBlobContainerName);
        container.CreateIfNotExistsAsync().Wait();

        var blobid = "workaround";
        var blob = container.GetBlockBlobReference(blobid);

        var contentHash = Convert.ToBase64String(MD5.Create().ComputeHash(_buffer, 0, _buffer.Length));

        var tasks = new Task[_blockNum];
        var error412Count = 0;
        var error400Count = 0;
        var hashSet = new ConcurrentBag<string>();
        for (int reps = 0; reps < _repetitionNum; reps++)
        {
            Task.Delay(100).Wait();
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var operatingBlock = GetNewBlockId();
                    hashSet.Add(operatingBlock);
                    while (true)
                    {
                        try
                        {
                            var (etag, date, blockIds) = await GetAllBlockIdsAsync(blob);

                            await blob.PutBlockAsync(
                                blockId: operatingBlock,
                                blockData: new MemoryStream(_buffer, 0, _buffer.Length, writable: false),
                                contentMD5: contentHash,
                                accessCondition: AccessCondition.GenerateEmptyCondition(),
                                options: new BlobRequestOptions()
                                {
                                    StoreBlobContentMD5 = true,
                                    UseTransactionalMD5 = true
                                },
                                operationContext: new OperationContext(),
                                cancellationToken: default).ConfigureAwait(false);

                            if (!blockIds.Contains(operatingBlock))
                                blockIds.Add(operatingBlock);

                            await blob.PutBlockListAsync(
                                blockIds,
                                accessCondition: AccessCondition.GenerateIfMatchCondition(etag),
                                options: new BlobRequestOptions(),
                                operationContext: new OperationContext(),
                                cancellationToken: default).ConfigureAwait(false);
                            
                            var (_, _, commitedBlocks) = await GetAllBlockIdsAsync(blob);
                            if (Enumerable.SequenceEqual(blockIds, commitedBlocks))
                                break;
                        }
                        catch (StorageException e)
                        {
                            if (e.RequestInformation.HttpStatusCode == 412)
                            {
                                error412Count++;
                            }
                            else if (e.RequestInformation.HttpStatusCode == 400)
                            {
                                error400Count++;
                            }
                            else
                                throw;
                        }
                        if (error400Count > 100)
                            break;
                    }
                });
            }
            Task.WaitAll(tasks);
        }

        var blocks = await blob.DownloadBlockListAsync();

        Assert.Equal(_blockNum * _repetitionNum, hashSet.ToArray().Length);
        Assert.Equal((0, 0, _blockNum * _repetitionNum), (error400Count, error412Count, blocks.ToArray().Length));
    }

    [Fact]
    public async void parallel_writing_wished()
    {
        var client = CloudStorageAccount.Parse(_connectionString).CreateCloudBlobClient();
        var logBlobContainerName = "dev-" + Guid.NewGuid().ToString();
        var container = client.GetContainerReference(logBlobContainerName);
        container.CreateIfNotExistsAsync().Wait();

        var blobid = "wished";
        var blob = container.GetBlockBlobReference(blobid);

        var contentHash = Convert.ToBase64String(MD5.Create().ComputeHash(_buffer, 0, _buffer.Length));

        var tasks = new Task[_blockNum];
        var error412Count = 0;
        var error400Count = 0;
        var isInfinite = false;
        
        int blockIdLength = GetNewBlockId().Length;

        for (int i = 0; i < _blockNum; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var operatingBlock = GetNewBlockId();
                
                // Simple check to verify whether block ids have the same length.
                if (blockIdLength != operatingBlock.Length)
                    throw new Exception("Block id lengths are different.");

                while (true)
                {
                    var (etag, date, blockIds) = await GetAllBlockIdsAsync(blob);

                    await blob.PutBlockAsync(
                        blockId: operatingBlock,
                        blockData: new MemoryStream(_buffer, 0, _buffer.Length, writable: false),
                        contentMD5: contentHash,
                        accessCondition: AccessCondition.GenerateEmptyCondition(),
                        options: new BlobRequestOptions()
                        {
                            StoreBlobContentMD5 = true,
                            UseTransactionalMD5 = true
                        },
                        operationContext: new OperationContext(),
                        cancellationToken: default).ConfigureAwait(false);

                    try
                    {
                        if (!blockIds.Contains(operatingBlock))
                            blockIds.Add(operatingBlock);

                        await blob.PutBlockListAsync(
                            blockIds,
                            accessCondition: AccessCondition.GenerateIfMatchCondition(etag),
                            options: new BlobRequestOptions(),
                            operationContext: new OperationContext(),
                            cancellationToken: default).ConfigureAwait(false);
                        break;
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode == 412)
                        {
                            error412Count++;
                        }
                        else if (e.RequestInformation.HttpStatusCode == 410)
                        {
                            error400Count++;
                        }
                        else
                            throw;
                    }
                    if (error400Count > 100)
                    {
                        isInfinite = true;
                        break;
                    }
                }
            });
        }

        Task.WaitAll(tasks);
        if (isInfinite)
            throw new Exception("Execution resulted in the infinite loop on error 400.");

        var blocks = await blob.DownloadBlockListAsync();
        Assert.Equal(_blockNum, blocks.ToArray().Length);
    }

    private async Task<(Azure.ETag?, DateTimeOffset?, List<string>)> NewGetAllBlockIdsAsync(BlockBlobClient blob)
    {
        try
        {
            var blockItems = await blob.GetBlockListAsync();
            var etag = blob.GetProperties().Value.ETag;
            var lastModified = blob.GetProperties().Value.LastModified;
            var blockIds = blockItems.Value.CommittedBlocks.Select(b => b.Name).ToList();
            return (etag, lastModified, blockIds);
        }
        // Storage exception can be thrown if there is no any written blob.
        catch (Azure.RequestFailedException e)
        {
            if (e.Status == 404)
                return (null, null, new List<string>());
            else
                throw;
        }
    }

    [Fact]
    public async void new_library_workaround()
    {
        var blobid = "newlib";
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new-" + Guid.NewGuid().ToString();
        var containerClient = await blobServiceClient.CreateBlobContainerAsync(containerName);
        var contentHash = MD5.Create().ComputeHash(_buffer, 0, _buffer.Length);
        var blob = containerClient.Value.GetBlockBlobClient(blobid);
        blob.CommitBlockList(new List<string>());
        
        var tasks = new Task[_blockNum];
        var error400 = 0;
        var error412 = 0;

        for (int reps = 0; reps < _repetitionNum; reps++)
        {
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var stream = new MemoryStream(_buffer);
                    var blockid = GetNewBlockId();
                    var conditions = new BlobRequestConditions();
                    
                    while (true)
                    {
                        try
                        {
                            stream.Position = 0;
                            var (etag, lastModified, commitedBlocks) = await NewGetAllBlockIdsAsync(blob);

                            // stage block
                            await blob.StageBlockAsync(blockid, stream, contentHash);

                            if (!commitedBlocks.Contains(blockid))
                                commitedBlocks.Add(blockid);

                            conditions.IfMatch = etag;
                            conditions.IfUnmodifiedSince = lastModified;

                            // commit block
                            await blob.CommitBlockListAsync(
                                commitedBlocks,
                                conditions: conditions);
                            break;
                        }
                        catch (Azure.RequestFailedException e)
                        {
                            if (e.Status == 412)
                            {
                                error412++;
                            }
                            else if (e.Status == 400)
                            {
                                error400++;
                            }
                            else
                            {
                                throw;
                            }
                        }
                    }
                });
            }
            Task.WaitAll(tasks);
        }

        var blocks = await blob.GetBlockListAsync();

        Assert.Equal(
            (0, 0, _blockNum * _repetitionNum), 
            (error400, error412, blocks.Value.CommittedBlocks.ToArray().Length));
    }


    [Fact]
    public async void new_library_wished()
    {
        var blobid = "newlib";
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new-" + Guid.NewGuid().ToString();
        var containerClient = await blobServiceClient.CreateBlobContainerAsync(containerName);
        var blob = containerClient.Value.GetBlockBlobClient(blobid);
        var contentHash = MD5.Create().ComputeHash(_buffer, 0, _buffer.Length);
        blob.CommitBlockList(new List<string>());

        var tasks = new Task[_blockNum];
        int blockIdLength = GetNewBlockId().Length;

        for (int repetition = 0; repetition < _repetitionNum; repetition++)
        {
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var stream = new MemoryStream(_buffer);
                    var blockid = GetNewBlockId();
                    
                    if (blockIdLength != blockid.Length)
                        throw new Exception("Different block id length.");

                    var conditions = new BlobRequestConditions();
                    var (etag, lastModified, commitedBlocks) = await NewGetAllBlockIdsAsync(blob);

                    // stage block
                    await blob.StageBlockAsync(blockid, stream, contentHash, conditions);

                    while (true)
                    {
                        try
                        {
                            if (!commitedBlocks.Contains(blockid))
                                commitedBlocks.Add(blockid);

                            conditions.IfMatch = etag;
                            conditions.IfUnmodifiedSince = lastModified;

                            // commit block
                            await blob.CommitBlockListAsync(
                                commitedBlocks,
                                conditions: conditions);
                            break;
                        }
                        catch (Azure.RequestFailedException e)
                        {
                            if (e.Status == 412)
                            {
                                (etag, lastModified, commitedBlocks) = await NewGetAllBlockIdsAsync(blob);
                            }
                            else
                                throw;
                        }
                    }

                });
            }
        }
        

        Task.WaitAll(tasks);

        var blocks = await blob.GetBlockListAsync();
        Assert.Equal(_blockNum, actual: blocks.Value.CommittedBlocks.ToArray().Length);
    }

    public void Dispose()
    {
    }
}