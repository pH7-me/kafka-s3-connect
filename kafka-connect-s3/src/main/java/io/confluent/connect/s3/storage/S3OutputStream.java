/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.services.s3.model.UploadPartRequest;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.common.util.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Output stream enabling multi-part uploads of Kafka records.
 *
 * <p>The implementation has borrowed the general structure of Hadoop's implementation.
 */
public class S3OutputStream extends PositionOutputStream {
  private static final Logger log = LoggerFactory.getLogger(S3OutputStream.class);
  private final AmazonS3 s3;
  private final S3SinkConnectorConfig connectorConfig;
  private final String bucket;
  private final String key;
  private final String ssea;
  private final SSECustomerKey sseCustomerKey;
  private final String sseKmsKeyId;
  private final ProgressListener progressListener;
  private final int partSize;
  private final CannedAccessControlList cannedAcl;
  private boolean uploadParallely;
  private final int uploadParallelization;
  private boolean closed;
  private ByteBuf buffer;
  private MultipartUpload multiPartUpload;
  private final CompressionType compressionType;
  private final int compressionLevel;
  private volatile OutputStream compressionFilter;
  private Long position;

  public S3OutputStream(String key, S3SinkConnectorConfig conf, AmazonS3 s3) {
    this.s3 = s3;
    this.connectorConfig = conf;
    this.bucket = conf.getBucketName();
    this.key = key;
    this.ssea = conf.getSsea();
    final String sseCustomerKeyConfig = conf.getSseCustomerKey();
    this.sseCustomerKey =
        (SSEAlgorithm.AES256.toString().equalsIgnoreCase(ssea)
                && StringUtils.isNotBlank(sseCustomerKeyConfig))
            ? new SSECustomerKey(sseCustomerKeyConfig)
            : null;
    this.sseKmsKeyId = conf.getSseKmsKeyId();
    this.partSize = conf.getPartSize();
    this.cannedAcl = conf.getCannedAcl();
    this.uploadParallely = conf.getUploadParallelizationEnable();
    this.uploadParallelization = conf.getUploadParallelization();
    this.closed = false;

    final boolean elasticBufEnable = conf.getElasticBufferEnable();
    if (elasticBufEnable) {
      final int elasticBufInitialCap = conf.getElasticBufferInitCap();
      this.buffer = new ElasticByteBuffer(this.partSize, elasticBufInitialCap);
    } else {
      this.buffer = new SimpleByteBuf(this.partSize);
    }

    this.progressListener = new ConnectProgressListener();
    this.multiPartUpload = null;
    this.compressionType = conf.getCompressionType();
    this.compressionLevel = conf.getCompressionLevel();
    this.position = 0L;
    log.debug("Create S3OutputStream for bucket '{}' key '{}'", bucket, key);
  }

  @Override
  public void write(int b) throws IOException {
    buffer.put((byte) b);
    if (!buffer.hasRemaining()) {
      uploadPart();
    }
    position++;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (outOfRange(off, b.length) || len < 0 || outOfRange(off + len, b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (buffer.remaining() <= len) {
      int firstPart = buffer.remaining();
      buffer.put(b, off, firstPart);
      position += firstPart;
      uploadPart();
      write(b, off + firstPart, len - firstPart);
    } else {
      buffer.put(b, off, len);
      position += len;
    }
  }

  private static boolean outOfRange(int off, int len) {
    return off < 0 || off > len;
  }

  private void uploadPart() throws IOException {
    uploadPart(partSize);
    buffer.clear();
  }

  private void uploadPart(final int size) throws IOException {
    log.debug("New multi-part upload for bucket '{}' key '{}'", bucket, key);
    if (multiPartUpload == null) {
      multiPartUpload = newMultipartUpload();
    }
    try {
      multiPartUpload.uploadPart(new ByteArrayInputStream(buffer.array().clone()), size);
    } catch (Exception e) {
      if (multiPartUpload != null) {
        multiPartUpload.abort();
        log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
      }
      throw new IOException("Part upload failed: ", e);
    }
  }

  public void commit() throws IOException {
    if (closed) {
      log.warn(
          "Tried to commit data for bucket '{}' key '{}' on a closed stream. Ignoring.",
          bucket,
          key);
      return;
    }

    try {
      compressionType.finalize(compressionFilter);
      if (buffer.hasRemaining()) {
        uploadPart(buffer.position());
      }
      multiPartUpload.complete();
      log.debug("Upload complete for bucket '{}' key '{}'", bucket, key);
    } catch (IOException e) {
      log.error("Multipart upload failed to complete for bucket '{}' key '{}'", bucket, key);
      throw new ConnectException(
          String.format("Multipart upload failed to complete: %s", e.getMessage()));
    } finally {
      buffer.clear();
      multiPartUpload = null;
      internalClose();
    }
  }

  @Override
  public void close() throws IOException {
    internalClose();
  }

  private void internalClose() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    if (multiPartUpload != null) {
      multiPartUpload.abort();
      log.debug("Multipart upload aborted for bucket '{}' key '{}'.", bucket, key);
    }
    super.close();
  }

  private ObjectMetadata newObjectMetadata() {
    ObjectMetadata meta = new ObjectMetadata();
    if (StringUtils.isNotBlank(ssea)) {
      meta.setSSEAlgorithm(ssea);
    }
    return meta;
  }

  private MultipartUpload newMultipartUpload() throws IOException {
    InitiateMultipartUploadRequest initRequest =
        new InitiateMultipartUploadRequest(bucket, key, newObjectMetadata())
            .withCannedACL(cannedAcl);

    if (SSEAlgorithm.KMS.toString().equalsIgnoreCase(ssea) && StringUtils.isNotBlank(sseKmsKeyId)) {
      log.debug("Using KMS Key ID: {}", sseKmsKeyId);
      initRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(sseKmsKeyId));
    } else if (sseCustomerKey != null) {
      log.debug("Using KMS Customer Key");
      initRequest.setSSECustomerKey(sseCustomerKey);
    }

    try {
      return new MultipartUpload(s3.initiateMultipartUpload(initRequest).getUploadId());
    } catch (AmazonServiceException e) {
      if (e.getErrorType() == ErrorType.Client) {
        // S3 documentation states that this error type means there is a problem with the request
        // and that retrying this request will not result in a successful response. This includes
        // errors such as incorrect access keys, invalid parameter values, missing parameters, etc.
        // Therefore, the connector should propagate this exception and fail.
        throw new ConnectException("Unable to initiate MultipartUpload", e);
      }
      throw new IOException("Unable to initiate MultipartUpload.", e);
    } catch (AmazonClientException e) {
      throw new IOException("Unable to initiate MultipartUpload.", e);
    }
  }

  private class MultipartUpload {
    private final String uploadId;
    private final List<PartETag> partETags;
    private List<Future<PartETag>> partETagsFut;
    private AtomicInteger requests;
    private ExecutorService pool;

    public MultipartUpload(String uploadId) {
      this.uploadId = uploadId;
      this.partETags = new ArrayList<>();
      if (uploadParallely) {

        this.partETagsFut = new ArrayList<>();
        this.requests = new AtomicInteger(0);
        // Initialize threadpool for multipart uploads
        this.pool =
            new ThreadPoolExecutor(
                uploadParallelization,
                uploadParallelization,
                1,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(uploadParallelization),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        log.debug(
            "Initiated multi-part upload for bucket '{}' key '{}' with id '{}' "
                + "and parallelization of '{}'",
            bucket,
            key,
            uploadId,
            uploadParallelization);

      } else {

        log.debug(
            "Initiated multi-part upload for bucket '{}' key '{}' with id '{}'",
            bucket,
            key,
            uploadId);
      }
    }

    public void uploadPart(ByteArrayInputStream inputStream, int partSize) {
      if (uploadParallely) {
        uploadPartParallely(inputStream, partSize);
      } else {
        uploadPartSerially(inputStream, partSize);
      }
    }

    public void uploadPartSerially(ByteArrayInputStream inputStream, int partSize) {
      int currentPartNumber = partETags.size() + 1;
      UploadPartRequest request =
          new UploadPartRequest()
              .withBucketName(bucket)
              .withKey(key)
              .withUploadId(uploadId)
              .withSSECustomerKey(sseCustomerKey)
              .withInputStream(inputStream)
              .withPartNumber(currentPartNumber)
              .withPartSize(partSize)
              .withGeneralProgressListener(progressListener);
      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      partETags.add(s3.uploadPart(request).getPartETag());
    }

    public void uploadPartParallely(ByteArrayInputStream inputStream, int partSize) {
      final int currentPartNumber = requests.incrementAndGet();
      Future<PartETag> task =
          pool.submit(
              () -> {
                log.debug("Starting multipart upload request {} ", currentPartNumber);
                UploadPartRequest request =
                    new UploadPartRequest()
                        .withBucketName(bucket)
                        .withKey(key)
                        .withUploadId(uploadId)
                        .withSSECustomerKey(sseCustomerKey)
                        .withInputStream(inputStream)
                        .withPartNumber(currentPartNumber)
                        .withPartSize(partSize)
                        .withGeneralProgressListener(progressListener);
                return s3.uploadPart(request).getPartETag();
              });
      log.debug("Uploading part {} for id '{}'", currentPartNumber, uploadId);
      partETagsFut.add(task);
    }

    public void complete() {
      log.info("Completing multi-part upload for key '{}', id '{}'", key, uploadId);
      if (uploadParallely) {

        ArrayList<PartETag> tags = new ArrayList<>();
        for (Future<PartETag> tagTask : partETagsFut) {
          try {
            tags.add(tagTask.get());
          } catch (Exception e) {
            log.error("Unable to get multipart upload result", e);
            //            throw e; // TODO should we throw?
          }
        }
        pool.shutdown();

        CompleteMultipartUploadRequest completeRequest =
            new CompleteMultipartUploadRequest(bucket, key, uploadId, tags);
        s3.completeMultipartUpload(completeRequest);

      } else {

        CompleteMultipartUploadRequest completeRequest =
            new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags);
        s3.completeMultipartUpload(completeRequest);
      }
    }

    public void abort() {
      log.warn("Aborting multi-part upload with id '{}'", uploadId);
      try {
        s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
      } catch (Exception e) {
        // ignoring failure on abort.
        log.warn("Unable to abort multipart upload, you may need to purge uploaded parts: ", e);
      } finally {
        if (pool != null) {
          pool.shutdown();
        }
      }
    }
  }

  public OutputStream wrapForCompression() {
    if (compressionFilter == null) {
      // Initialize compressionFilter the first time this method is called.
      compressionFilter = compressionType.wrapForOutput(this, compressionLevel);
    }
    return compressionFilter;
  }

  // Dummy listener for now, just logs the event progress.
  private static class ConnectProgressListener implements ProgressListener {
    public void progressChanged(ProgressEvent progressEvent) {
      log.debug("Progress event: " + progressEvent);
    }
  }

  public long getPos() {
    return position;
  }
}
