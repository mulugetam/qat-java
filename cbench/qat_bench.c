//
// gcc -I $QZ_ROOT/include -L $QZ_ROOT/src/.libs -Wl,-rpath=$QZ_ROOT/src/.libs -lqatzip -lnuma qat_bench.c -o qat_bench
// ./qat_bench 0 input_data.txt

#include "qatzip.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define HW_SW 0
#define COMPRESS_LEVEL 6
#define RETRY_COUNT 0

#define LOOP_COUNT 1000

static char *read_text_file(const char *file_name, int *file_size) {
  FILE *file = fopen(file_name, "r");
  if (file == NULL) {
    printf("Error: Unable to open the file.\n");
    return NULL;
  }

  // Find the size of the file
  fseek(file, 0, SEEK_END);
  long size = ftell(file);
  fseek(file, 0, SEEK_SET);

  // Allocate memory to store the file contents
  char *buffer = (char *)malloc(size + 1);
  if (buffer == NULL) {
    printf("Error: Memory allocation failed.\n");
    fclose(file);
    return NULL;
  }

  // Read the file into the buffer
  size_t bytes_read = fread(buffer, 1, size, file);
  if (bytes_read != size) {
    printf("Error: Reading the file failed.\n");
    fclose(file);
    free(buffer);
    return NULL;
  }

  *file_size = (int)size;

  // Close the file and return the buffer
  fclose(file);
  return buffer;
}

static int compress(QzSession_T *sess, unsigned char *src_ptr,
                    unsigned int src_len, unsigned char *dst_ptr,
                    unsigned int dst_len, int *src_read, int *dst_written,
                    int retry_count) {
  int status = qzCompress(sess, src_ptr, &src_len, dst_ptr, &dst_len, 1);
  if (status == QZ_NOSW_NO_INST_ATTACH && retry_count > 0) {
    while (retry_count > 0 && QZ_OK != status) {
      status = qzCompress(sess, src_ptr, &src_len, dst_ptr, &dst_len, 1);
      retry_count--;
    }
  }

  if (status != QZ_OK) {
    fprintf(stderr, "Error occurred while compressiong data. Status = %d.",
            status);
    return status;
  }

  *src_read = src_len;
  *dst_written = dst_len;

  return QZ_OK;
}

static int decompress(QzSession_T *sess, unsigned char *src_ptr,
                      unsigned int src_len, unsigned char *dst_ptr,
                      unsigned int dst_len, int *src_read, int *dst_written,
                      int retry_count) {
  int status = qzDecompress(sess, src_ptr, &src_len, dst_ptr, &dst_len);
  if (status == QZ_NOSW_NO_INST_ATTACH && retry_count > 0) {
    while (retry_count > 0 && QZ_OK != status && status != QZ_BUF_ERROR &&
           status != QZ_DATA_ERROR) {
      status = qzDecompress(sess, src_ptr, &src_len, dst_ptr, &dst_len);
      retry_count--;
    }
  }
  if (status != QZ_OK && status != QZ_BUF_ERROR && status != QZ_DATA_ERROR) {
    fprintf(stderr, "Error occurred while decompressing data. Status = %d.",
            status);
    return status;
  }

  *src_read = src_len;
  *dst_written = dst_len;

  return QZ_OK;
}

static int create_deflate_session(QzSession_T *qz_session) {
  int rc = qzInit(qz_session, HW_SW);
  if (rc != QZ_OK) {
    fprintf(stderr, "init failed. error code was %d.\n", rc);
    return 1;
  }

  QzSessionParamsDeflate_T deflate_params;
  rc = qzGetDefaultsDeflate(&deflate_params);
  if (rc != QZ_OK) {
    fprintf(stderr, "getting deflate defaults failed.\n");
    return 1;
  }

  deflate_params.data_fmt = QZ_DEFLATE_GZIP_EXT;
  deflate_params.common_params.polling_mode = QZ_BUSY_POLLING;
  deflate_params.common_params.comp_lvl = COMPRESS_LEVEL;
  rc = qzSetupSessionDeflate(qz_session, &deflate_params);
  if (rc != QZ_OK) {
    fprintf(stderr, "setting up a deflate session failed. error code was %d\n",
            rc);
    return 1;
  }

  return QZ_OK;
}

static int create_lz4_session(QzSession_T *qz_session) {
  int rc = qzInit(qz_session, HW_SW);
  if (rc != QZ_OK) {
    fprintf(stderr, "init failed. error code was %d.\n", rc);
    return 1;
  }

  QzSessionParamsLZ4_T lz4_params;
  rc = qzGetDefaultsLZ4(&lz4_params);
  if (rc != QZ_OK) {
    fprintf(stderr, "getting lz4 defaults failed.\n");
    return 1;
  }

  lz4_params.common_params.polling_mode = QZ_BUSY_POLLING;
  lz4_params.common_params.comp_lvl = COMPRESS_LEVEL;
  rc = qzSetupSessionLZ4(qz_session, &lz4_params);
  if (rc != QZ_OK) {
    fprintf(stderr, "setting up a lz4 session failed. error code was %d\n", rc);
    return 1;
  }

  return QZ_OK;
}

static int benchmark_compression(QzSession_T *qz_session, char *in_buf,
                                 int in_buf_len, char *out_buf,
                                 int out_buf_len) {
  char *src_ptr = NULL;
  char *dst_ptr = NULL;

  int bytes_read = 0;
  int bytes_written = 0;
  for (int i = 0; i < LOOP_COUNT; ++i) {
    src_ptr = in_buf;
    dst_ptr = out_buf;
    compress(qz_session, src_ptr, in_buf_len, dst_ptr, out_buf_len, &bytes_read,
             &bytes_written, RETRY_COUNT);
  }

  // Benchmark compression
  struct timespec s_time, e_time;
  timespec_get(&s_time, TIME_UTC);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    src_ptr = in_buf;
    dst_ptr = out_buf;
    compress(qz_session, src_ptr, in_buf_len, dst_ptr, out_buf_len, &bytes_read,
             &bytes_written, RETRY_COUNT);
  }
  timespec_get(&e_time, TIME_UTC);

  // Report compression result
  long long elapsed_ns = (e_time.tv_sec - s_time.tv_sec) * 1000000000LL;
  elapsed_ns += (e_time.tv_nsec - s_time.tv_nsec);
  double elapsed_s = elapsed_ns * 1e-9;

  double c_speed = (LOOP_COUNT * 1e0 * bytes_read) / (1024 * 1024 * elapsed_s);
  double ratio = in_buf_len * 1e0 / bytes_written;

  printf("Compressed size: %d, compression speed (MB/sec): %.2f, compression "
         "ratio: %.2f\n",
         bytes_written, c_speed, ratio);

  return bytes_written;
}

static int benchmark_decompression(QzSession_T *qz_session, char *in_buf,
                                   int in_buf_len, char *out_buf,
                                   int out_buf_len) {
  char *src_ptr = NULL;
  char *dst_ptr = NULL;

  int bytes_read = 0;
  int bytes_written = 0;
  for (int i = 0; i < LOOP_COUNT; ++i) {
    src_ptr = in_buf;
    dst_ptr = out_buf;
    decompress(qz_session, src_ptr, in_buf_len, dst_ptr, out_buf_len,
               &bytes_read, &bytes_written, RETRY_COUNT);
  }

  // Benchmark compression
  struct timespec s_time, e_time;
  timespec_get(&s_time, TIME_UTC);
  for (int i = 0; i < LOOP_COUNT; ++i) {
    src_ptr = in_buf;
    dst_ptr = out_buf;
    decompress(qz_session, src_ptr, in_buf_len, dst_ptr, out_buf_len,
               &bytes_read, &bytes_written, RETRY_COUNT);
  }
  timespec_get(&e_time, TIME_UTC);

  // Report decompression result
  long long elapsed_ns = (e_time.tv_sec - s_time.tv_sec) * 1000000000LL;
  elapsed_ns += (e_time.tv_nsec - s_time.tv_nsec);
  double elapsed_s = elapsed_ns * 1e-9;

  // out_buf_len is the original size
  double dec_speed =
      (LOOP_COUNT * 1e0 * bytes_written) / (1024 * 1024 * elapsed_s);

  printf("Decompressed size: %d, decompression speed (MB/sec): %.2f\n",
         bytes_written, dec_speed);

  return bytes_written;
}

int main(int argc, char *argv[]) {
  setbuf(stdout, NULL);

  if (argc < 3) {
    fprintf(stderr, "Invalid number of arguments.");
    return 1;
  }

  QzSession_T *qz_session = calloc(1, sizeof(QzSession_T));

  int algorithm = atoi(argv[1]);
  if (algorithm == 0)
    create_deflate_session(qz_session);
  else
    create_lz4_session(qz_session);
  
  printf("Compression algorithm: %s\n", algorithm == 0 ? "DEFLATE": "LZ4");

  char *file_name = argv[2];

  // Read file into an input buffer
  int in_buf_len = 0;
  char *in_buf = read_text_file(file_name, &in_buf_len);

  // Prepare output buffer
  int out_buf_len = qzMaxCompressedLength(in_buf_len, qz_session);
  char *out_buf = malloc(out_buf_len);

  printf("File %s read. Size is %d. Output buffer size is %d.\n", file_name,
         in_buf_len, out_buf_len);

  printf("Benchmarking compression...\n");
  int comp_size = benchmark_compression(qz_session, in_buf, in_buf_len, out_buf,
                                        out_buf_len);

  printf("Benchmarking decompression...\n");
  benchmark_decompression(qz_session, out_buf, comp_size, in_buf, in_buf_len);

  // clean up
  free(in_buf);
  free(out_buf);

  qzTeardownSession(qz_session);
}
