#include "sys/socket.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include "linux/if_alg.h"
#include "iostream"
#include "string.h"
#include "unistd.h"
#include "fcntl.h"
#include "sys/time.h"
#include "sys/syscall.h"
#include "assert.h"

using namespace std;

static uint64_t now_usec()
{
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return tv.tv_sec*1000000 + tv.tv_usec;
}


class Sink {
public:
  virtual ~Sink() = default;

  virtual int get_fd() = 0;
  virtual const char* get_name() const = 0;
  virtual void clean(size_t len) = 0;
};


class PipeSink : public Sink {
  const size_t min_pipe_size;
  int fds[2];
  int null_fd;

public:
  PipeSink(const size_t min_pipe_size)
    : min_pipe_size(min_pipe_size) {
    int r = ::pipe(fds);
    assert(r == 0);

    r = ::fcntl(fds[1], F_SETPIPE_SZ, min_pipe_size);
    assert(r >= min_pipe_size);

    null_fd = ::open("/dev/null", O_WRONLY);
    assert(null_fd > 0);
  }

  ~PipeSink() {
    ::close(null_fd);

    ::close(fds[0]);
    ::close(fds[1]);
  }

  int get_fd() override {
    return fds[1];
  }

  const char* get_name() const override {
    return "pipe";
  }

  void clean(const size_t size) override {
    int r = ::splice(fds[0], nullptr, null_fd, nullptr, size, SPLICE_F_MOVE);
    assert(size == r);
  }
};


class FileSink : public Sink {
  int file_fd;

public:
  FileSink() {
    file_fd = ::open("/tmp/mem2fd", O_WRONLY | O_CREAT, S_IWUSR);
    //file_fd = ::open("/run/user/1000/mem2fd", O_WRONLY | O_CREAT, S_IWUSR);
    assert(file_fd > 0);
  }

  ~FileSink() {
    ::close(file_fd);
  }

  int get_fd() override {
    return file_fd;
  }

  const char* get_name() const override {
    return "file";
  }

  void clean(const size_t) override {
    off_t r = lseek(file_fd, 0, SEEK_SET);
    assert(r == 0);
  }
};

class SockSink : public Sink {
  int fds[2];
  int clean_fds[2];
  int null_fd;

  static int tcp_pipe(int (&fds)[2]) {
    int source_fd = -1;
    int sink_fd = -1;
    int server_fd = -1;
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(struct sockaddr_in));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = 0;
    try {
      server_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (server_fd < 0) {
        throw (__LINE__);
      }
      int enable = 1;
      if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) != 0) {
        throw __LINE__;
      }
      if (bind(server_fd, (const struct sockaddr*) &server_addr, sizeof(server_addr)) != 0) {
        throw __LINE__;
      }
      socklen_t len = sizeof(sockaddr_in);
      if (getsockname(server_fd, (struct sockaddr*)&server_addr, &len) != 0) {
        throw __LINE__;
      }
      if(server_addr.sin_port == 0) {
        //derr << "Port == 0" << dendl;
      }
      int ret =0;
#if 0
      int bufsize = 4194304000;

      ret = setsockopt(server_fd, SOL_SOCKET, SO_RCVBUF, &bufsize,
                       sizeof(bufsize));
      assert(0 == ret);

      ret = setsockopt(server_fd, SOL_SOCKET, SO_SNDBUF, &bufsize,
                       sizeof(bufsize));
#endif
      assert(0 == ret);
      if (listen(server_fd, 1) != 0) {
        throw __LINE__;
      }
      sink_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (sink_fd < 0) {
        throw __LINE__;
      }

#if 0
      ret = setsockopt(sink_fd, SOL_SOCKET, SO_RCVBUF, &bufsize,
                       sizeof(bufsize));
      assert(0 == ret);

      ret = setsockopt(sink_fd, SOL_SOCKET, SO_SNDBUF, &bufsize,
                       sizeof(bufsize));
      assert(0 == ret);
#endif


      if (fcntl(sink_fd, F_SETFL, fcntl(sink_fd, F_GETFL,0) | O_NONBLOCK) !=0) {
        throw __LINE__;
      }
      if (connect(sink_fd, (const struct sockaddr*) &server_addr, sizeof(server_addr)) != -1) {
        throw __LINE__;
      }
      if (errno != EINPROGRESS) {
        throw __LINE__;
      }
      source_fd = accept(server_fd, nullptr, nullptr);
      if (source_fd < 0) {
        throw __LINE__;
      }
      if (fcntl(sink_fd, F_SETFL, fcntl(sink_fd, F_GETFL,0) & ~O_NONBLOCK) != 0) {
        throw __LINE__;
      }

      ::close(server_fd);

#if 0
      ret = setsockopt(source_fd, SOL_SOCKET, SO_RCVBUF, &bufsize,
                       sizeof(bufsize));

      ret = setsockopt(source_fd, SOL_SOCKET, SO_SNDBUF, &bufsize,
                       sizeof(bufsize));
      assert(0 == ret);
#endif

      fds[1] = sink_fd;
      fds[0] = source_fd;
      return 0;

    } catch (int line) {
      //derr << "Error tcp_pipe at" << line << dendl;
      printf("ka boom in line: %d, errno=%d\n", line, errno);
      if (sink_fd>=0) {
        close(sink_fd);
      }
      if (source_fd>=0) {
        close(source_fd);
      }
      if (server_fd>=0) {
        close(server_fd);
      }
      return -1;
    }
  }

public:
  SockSink(const size_t chunk_size) {
    int r = tcp_pipe(fds);
    assert(r == 0);

    r = ::pipe(clean_fds);
    assert(r == 0);
    r = ::fcntl(clean_fds[1], F_SETPIPE_SZ, chunk_size);
    assert(r >= chunk_size);

    null_fd = ::open("/dev/null", O_WRONLY);
    assert(null_fd > 0);
  }

  ~SockSink() {
    ::close(null_fd);

    ::close(clean_fds[0]);
    ::close(clean_fds[1]);

    ::close(fds[0]);
    ::close(fds[1]);
  }

  int get_fd() override {
    return fds[1];
  }

  const char* get_name() const override {
    return "sock";
  }

  void clean(const size_t size) override {
    char junk[size];

#if 1
    size_t r = read(fds[0], junk, size);
    assert(r == size);
#else
    size_t r = ::splice(fds[0], nullptr, clean_fds[1], nullptr, size, SPLICE_F_MOVE);
    assert(size == r);

    r = ::splice(clean_fds[0], nullptr, null_fd, nullptr, size, SPLICE_F_MOVE);
    assert(size == r);
#endif
  }
};


class Clock
{
  friend class Range;

  uint64_t t = 0;
  uint64_t wall_t = 0;

public:
  class Range;

  uint64_t get_wall() const {
    return wall_t;
  }
};

class Clock::Range {
  static uint64_t now_thread_usec() {
    struct timespec x;
    ::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &x);
    return x.tv_sec * 1000000000L + x.tv_nsec;
  }

  static uint64_t now_wall_usec()
  {
    struct timespec x;
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &x);
    return x.tv_sec * 1000000000L + x.tv_nsec;
  }

public:
  Clock* const p;

  Range(Clock* p) : p(p) {
    p->t -= now_thread_usec();
    p->wall_t -= now_wall_usec();
  }

  ~Range() {
    {
      uint64_t m1 = now_thread_usec();
      uint64_t m2 = now_thread_usec();
      p->t += m1 - (m2 - m1);
    }
    {
      uint64_t m1 = now_wall_usec();
      uint64_t m2 = now_wall_usec();
      p->wall_t += m1 - (m2 - m1);
    }
  }
};

class Feeder {
  Clock feeding_clk;

  virtual size_t write(int fd, void* buf, size_t len) = 0;

public:
  virtual ~Feeder() = default;
  virtual size_t write(Sink& sink, void* const buf, const size_t len) final {
    Clock::Range range(&feeding_clk);
    return write(sink.get_fd(), buf, len);
  }

  virtual uint64_t get_feeding_time() const {
    return feeding_clk.get_wall();
  };

  virtual const char* get_name() const = 0;
};


class WriteFeeder : public Feeder {
public:
  size_t write(const int fd, void* const buf, const size_t size) override {
    return ::write(fd, buf, size);
  }

  const char* get_name() const override {
    return "write";
  }
};

class WriteVFeeder : public Feeder  {
public:
  size_t write(const int fd, void* const buf, const size_t size) override {
    struct iovec vec = {
      .iov_base = buf,
      .iov_len  = size,
    };

    return ::writev(fd, &vec, 1);
  }

  const char* get_name() const override {
    return "writev";
  }
};

class VMSpliceFeeder : public Feeder {
protected:
  int fds[2];

  size_t do_vmsplice(const int fd,
                     void* const buf,
                     const size_t size,
                     const unsigned int flags) {
    const struct iovec vec = {
      .iov_base = buf,
      .iov_len  = size
    };

    int r = ::vmsplice(fds[1], &vec, 1, flags | SPLICE_F_MOVE);// | SPLICE_F_MORE);
    assert(size == r);

    r = ::splice(fds[0], nullptr, fd, nullptr, size,
                 SPLICE_F_MOVE);// | SPLICE_F_MORE);
    assert(size == r);
    return r;
  }

  size_t write(const int fd, void* const buf, const size_t size) override {
    return do_vmsplice(fd, buf, size, 0);
  }

public:
  VMSpliceFeeder(const size_t max_size) {
    int r = ::pipe(fds);
    assert(r == 0);

    r = ::fcntl(fds[1], F_SETPIPE_SZ, max_size);
    assert(r >= max_size);
  }

  ~VMSpliceFeeder() override {
    ::close(fds[0]);
    ::close(fds[1]);
  }

  const char* get_name() const override {
    return "vmsplice_0";
  }
};

class GiftingVMSpliceFeeder : public VMSpliceFeeder {
  size_t write(const int fd, void* const buf, const size_t size) override {
    return do_vmsplice(fd, buf, size, SPLICE_F_GIFT);
  }

public:
  using VMSpliceFeeder::VMSpliceFeeder;

  const char* get_name() const override {
    return "vmsplice_gift";
  }
};


int main(int argc, char** argv)
{
  uint64_t chunk_size = 1024;
  int progression = 0;

  printf(       "               "
                " ----total-for-all-executed-jobs----"
                " ------per-single-executed-job-----\n");
  printf("%10s %14s %10s %8s %14s %14s\n",
         "sink", // 10
         "feeder", // 14
         "chunk size", // 8
         "iters", // 9
         "transfered", // 14
         "iter time");
  printf(
         "                            (bytes)"
         "            (MiB)          (MiB/s)     (usec)\n");

  static constexpr size_t mem_mul = 10;
  do /* chunk size */ {
    void *mem = aligned_alloc(4096, chunk_size * mem_mul + 4096);
    assert(mem != nullptr);

    /* Sinks. */
    PipeSink pipe_sink(chunk_size);
    SockSink sock_sink(chunk_size);
    FileSink file_sink;
    std::initializer_list<Sink*> sinks = {
      &pipe_sink, &sock_sink, &file_sink,
    };

    /* Feeders. */
    WriteFeeder write_fed;
    WriteVFeeder writev_fed;
    VMSpliceFeeder vmsplice0_fed(chunk_size);
    GiftingVMSpliceFeeder vmsplice_gift_fed(chunk_size);

    std::initializer_list<Feeder*> feeders = {
      &write_fed, &writev_fed, &vmsplice0_fed, &vmsplice_gift_fed
    };

    for (auto sink : sinks) {
      for (auto feeder : feeders) {
        const uint64_t start = now_usec();
        uint64_t no_tests = 0, end = 0;

        do /* time */ {
          void *wrk_mem = static_cast<char*>(mem)
                        + chunk_size * (no_tests % mem_mul);
          size_t written = feeder->write(*sink, wrk_mem, chunk_size);
          assert(chunk_size == written);

          sink->clean(chunk_size);

          no_tests++;
          end = now_usec();
        } while (end - start < 1 * 1000 * 1000);

        const double total_bytes = static_cast<double>(chunk_size) * no_tests;
        const double total_mbytes = total_bytes / (1024 * 1024);

        const double total_secs = static_cast<double>(feeder->get_feeding_time())
                                / (1000.0 * 1000.0 * 1000.0);
        printf("%10s %14s %10lu %8lu %9.2lf %14.2lf %14.3lf\n",
               sink->get_name(),
               feeder->get_name(),
               chunk_size,
               no_tests,
               total_mbytes,
               total_mbytes / total_secs,
               (double)(feeder->get_feeding_time())/no_tests);
      }
    }

    free(mem);

    printf("-------\n");

#if 0
    const double next[4] = { 5./4, 6./5, 7./6, 8./7 };
    chunk_size = chunk_size * next[progression];
    progression = (progression + 1) % 4 ;
#else
    chunk_size *= 2;
#endif
  } while(chunk_size < 100000000000);
}
