namespace ProofOfConcept;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Azure.Storage.Blobs.Models;
using Azure;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;


public class proof_of_concept : IDisposable
{
    private const string _connectionString =
        "DefaultEndpointsProtocol=https;AccountName=bigfilestest;AccountKey=YvclLEtAM0FvgBkgbTQV6Xch97GOMxgR2fanGm/WSIJkOfVZ7oTEWHwxeXegUBR39oKdSFqahpugGAx5dknpIQ==";

    private int _blockNum = 3;

    private int _repetitionNum = 4;

    private byte[] _buffer;

    public proof_of_concept()
    {
        _buffer = new byte[1024 * 1024 * 4];
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
    public async void parallel_writing_workaround()
    {
        var blobid = "newlib" + Guid.NewGuid().ToString();
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new";

        var client = CloudStorageAccount.Parse(_connectionString).CreateCloudBlobClient();
        var container = client.GetContainerReference(containerName);
        container.CreateIfNotExistsAsync().Wait();

        var blob = container.GetBlockBlobReference(blobid);

        var contentHash = Convert.ToBase64String(MD5.Create().ComputeHash(_buffer, 0, _buffer.Length));

        var tasks = new Task[_blockNum];
        var error412Count = 0;
        var error400Count = 0;
        for (int reps = 0; reps < _repetitionNum; reps++)
        {
            Task.Delay(100).Wait();
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var operatingBlock = GetNewBlockId();
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

        Assert.Equal((0, 0, _blockNum * _repetitionNum), (error400Count, error412Count, blocks.ToArray().Length));
    }

    [Fact]
    public async void parallel_writing_wished()
    {
        var blobid = "newlib" + Guid.NewGuid().ToString();
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new";

        var client = CloudStorageAccount.Parse(_connectionString).CreateCloudBlobClient();
        var container = client.GetContainerReference(containerName);
        container.CreateIfNotExistsAsync().Wait();

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

    private async Task<(Azure.ETag?, DateTimeOffset?, List<string>, List<string>)> NewGetAllBlockIdsAsync(BlockBlobClient blob)
    {
        try
        {
            var props = blob.GetProperties();
            var etag = props.Value.ETag;
            var lastModified = props.Value.LastModified;
            var blockItems = await blob.GetBlockListAsync();
            var commitedBlockIds = blockItems.Value.CommittedBlocks.Select(b => b.Name).ToList();
            var uncommitedBlockIds = blockItems.Value.UncommittedBlocks.Select(b => b.Name).ToList();

            return (etag, lastModified, commitedBlockIds, uncommitedBlockIds);
        }
        // Storage exception can be thrown if there is no any written blob.
        catch (Azure.RequestFailedException e)
        {
            if (e.Status == 404)
                return (null, null, new List<string>(), new List<string>());
            else
                throw;
        }
    }

    [Fact]
    public async void new_library_workaround()
    {
        var blobid = "newlib" + Guid.NewGuid().ToString();
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new";
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        var contentHash = MD5.Create().ComputeHash(_buffer, 0, _buffer.Length);
        var blob = containerClient.GetBlockBlobClient(blobid);
        blob.CommitBlockList(new List<string>());

        var tasks = new Task[_blockNum];
        var error400 = 0;
        var error412 = 0;
        var conditions = new BlobRequestConditions();

        for (int reps = 0; reps < _repetitionNum; reps++)
        {
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var stream = new MemoryStream(_buffer);
                    var blockid = GetNewBlockId();

                    while (true)
                    {
                        try
                        {
                            stream.Position = 0;
                            var (etag, lastModified, commitedBlocks, uncommitedBlocks) = await NewGetAllBlockIdsAsync(blob);
                            // In our application, any staged block will be commited,
                            // thus we wait until all blocks are commited.
                            if (uncommitedBlocks.Count != 0 && !uncommitedBlocks.Contains(blockid))
                                continue;

                            // stage block
                            await blob.StageBlockAsync(blockid, stream, contentHash);

                            if (!commitedBlocks.Contains(blockid))
                                commitedBlocks.Add(blockid);

                            conditions.IfMatch = etag;

                            // commit block
                            await blob.CommitBlockListAsync(
                                commitedBlocks,
                                conditions: conditions);

                            var (_, _, writtenBlocks, unwrittenBlocks) = await NewGetAllBlockIdsAsync(blob);
                            if (Enumerable.SequenceEqual(commitedBlocks, writtenBlocks))
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

        var query = blocks.Value.CommittedBlocks
            .Select(b => b.Name)
            .GroupBy(x => x)
            .Where(g => g.Count() > 1)
            .Select(y => y.Key)
            .ToList();

        Assert.Equal(0, query.Count);

        Assert.Equal(
            (0, 0, _blockNum * _repetitionNum),
            (error400, error412, blocks.Value.CommittedBlocks.ToArray().Length));
    }


    [Fact]
    public async void newlib_test_alg()
    {
        var blobid = "newlib" + Guid.NewGuid().ToString();
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new";
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        var contentHash = MD5.Create().ComputeHash(_buffer, 0, _buffer.Length);
        var blob = containerClient.GetBlockBlobClient(blobid);
        blob.CommitBlockList(new List<string>());

        var tasks = new Task[_blockNum];
        var error412 = 0;
        var conditions = new BlobRequestConditions();

        var sentBytes = 0;
        var resentBytes = 0;

        for (int reps = 0; reps < _repetitionNum; reps++)
        {
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(new Random(Guid.NewGuid().GetHashCode()).Next(0, 6000)));

                    var stream = new MemoryStream(_buffer);
                    var blockid = GetNewBlockId();
                    var conditions = new BlobRequestConditions();

                    var (etag, lastModified, blobCommitedBlocks, uncommitedBlocks) = await NewGetAllBlockIdsAsync(blob);
                    var shouldStage = true;
                    var sendingCounter = 0;
                    while (true)
                    {
                        stream.Position = 0;
                        try
                        {
                            if (shouldStage)
                            {
                                await blob.StageBlockAsync(blockid, stream, contentHash);
                                sentBytes += _buffer.Length;
                                if (sendingCounter > 0)
                                {
                                    resentBytes += _buffer.Length;
                                }
                            }
                            sendingCounter++;

                            if (!blobCommitedBlocks.Contains(blockid))
                                blobCommitedBlocks.Add(blockid);

                            conditions.IfMatch = etag;

                            await blob.CommitBlockListAsync(
                                blobCommitedBlocks,
                                conditions: conditions);
                            break;
                        }
                        catch (RequestFailedException e)
                        {
                            if (e.Status == 412)
                            {
                                error412++;

                                // Decide whether to repeat staging or to repeat commiting
                                (etag, lastModified, blobCommitedBlocks, uncommitedBlocks) = await NewGetAllBlockIdsAsync(blob);

                                if (uncommitedBlocks.Contains(blockid))
                                    shouldStage = false;
                                else
                                    shouldStage = true;
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
            (0, _blockNum * _repetitionNum),
            ((int)((float)resentBytes / (float)sentBytes * 100), blocks.Value.CommittedBlocks.ToArray().Length));

    }

    [Fact]
    public async void new_library_wished()
    {
        var blobid = "newlib" + Guid.NewGuid().ToString();
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new";
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
                    var (etag, lastModified, commitedBlocks, uncommitedBlocks) = await NewGetAllBlockIdsAsync(blob);

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
                                (etag, lastModified, commitedBlocks, uncommitedBlocks) = await NewGetAllBlockIdsAsync(blob);
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

    private async Task<BlobLease> AcquireLease(BlobLeaseClient leaseClient, string lid)
    {
        var conditions = new BlobRequestConditions();
        conditions.LeaseId = lid;
        var msa = 400;
        var msb = 1200;
        while (true)
        {
            try
            {
                return await leaseClient.AcquireAsync(TimeSpan.FromSeconds(15), conditions);
            }
            catch (RequestFailedException ex)
            {
                // The error 409 means that the blob is already leased and we have to wait.
                if (ex.Status != 409)
                    throw;
                else
                    await Task.Delay(TimeSpan.FromMilliseconds(new Random(Guid.NewGuid().GetHashCode()).Next(msa, msb)));
            }
        }
    }

    private async Task<List<string>> GetCommitedBlocks(BlockBlobClient blob)
    {
        try
        {
            var blockItems = await blob.GetBlockListAsync();
            var commitedBlockIds = blockItems.Value.CommittedBlocks.Select(b => b.Name).ToList();
            return commitedBlockIds;
        }
        // Storage exception can be thrown if there is no any written blob.
        catch (RequestFailedException e)
        {
            if (e.Status == 404)
                return new List<string>();
            else
                throw;
        }
    }

    [Fact]
    public async void newlib_sync_alg()
    {
        var blobid = "newlib" + Guid.NewGuid().ToString();
        var blobServiceClient = new BlobServiceClient(_connectionString);
        var containerName = "dev-new-1";
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        containerClient.CreateIfNotExists();
        var contentHash = MD5.Create().ComputeHash(_buffer, 0, _buffer.Length);
        var blob = containerClient.GetBlockBlobClient(blobid);
        blob.CommitBlockList(new List<string>());

        var tasks = new Task[_blockNum];

        for (int reps = 0; reps < _repetitionNum; reps++)
        {
            for (int i = 0; i < _blockNum; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var leaseClient = blob.GetBlobLeaseClient();
                    var blobLease = await AcquireLease(leaseClient, leaseClient.LeaseId);

                    var conditions = new BlobRequestConditions();
                    conditions.LeaseId = blobLease.LeaseId;

                    var commitedBlocks = await GetCommitedBlocks(blob);

                    var stream = new MemoryStream(_buffer);
                    var blockid = GetNewBlockId();

                    stream.Position = 0;

                    // stage block
                    await blob.StageBlockAsync(
                        blockid,
                        stream,
                        contentHash,
                        conditions: conditions);

                    if (!commitedBlocks.Contains(blockid))
                        commitedBlocks.Add(blockid);

                    // commit block
                    await blob.CommitBlockListAsync(
                        commitedBlocks,
                        conditions: conditions);

                    await leaseClient.ReleaseAsync();
                });
            }
            Task.WaitAll(tasks);
        }

        var blocks = await blob.GetBlockListAsync();

        Assert.Equal(_blockNum * _repetitionNum, blocks.Value.CommittedBlocks.ToArray().Length);
    }

    public void Dispose()
    {
    }
}