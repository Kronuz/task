/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// Process identity for a daemon (ported from Detent's detent_cli/daemon.cc, Detent:276bc58d):
// run_daemon forks into the background and reports startup to the caller, ProcessLock holds the
// pidfile's advisory lock, locked_pid names the process holding it. POSIX; standard library only.
// The caller chooses exit statuses: a held lock is ProcessLock::acquired() == false, a failed or
// timed-out startup is a thrown std::runtime_error.

#pragma once

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace task {

// A PID is reported only while the corresponding advisory lock is held.
inline std::optional<pid_t> locked_pid(const std::filesystem::path& path);

// Holds the pidfile's lock and writes this PID; acquired() is false when another process holds it.
class ProcessLock {
public:
	ProcessLock(const std::filesystem::path& path, bool system);
	~ProcessLock();
	ProcessLock(const ProcessLock&) = delete;
	ProcessLock& operator=(const ProcessLock&) = delete;
	bool acquired() const noexcept { return fd_ >= 0; }
private:
	int fd_ = -1;
};

using daemon_ready = std::function<void()>;
// Fork before constructing any threaded component. Startup errors propagate
// through a pipe; the parent returns 0 only after ready() and throws on an error or timeout.
// With detach false it runs `run` in place.
inline int run_daemon(bool detach, const std::filesystem::path& log_path,
                      const std::function<int(const daemon_ready&)>& run,
                      std::chrono::seconds startup_timeout = std::chrono::seconds(30));

namespace process_detail {
inline void fail(const char* operation) { throw std::runtime_error(std::string(operation) + ": " + std::strerror(errno)); }
inline int high_fd(int fd) {
	if (fd < 0 || fd >= 3) return fd;
	int copy = ::fcntl(fd, F_DUPFD_CLOEXEC, 3);
	int error = errno;
	::close(fd);
	errno = error;
	return copy;
}
struct Fd {
	int value;
	~Fd() { if (value >= 0) ::close(value); }
};
}  // namespace process_detail

inline std::optional<pid_t> locked_pid(const std::filesystem::path& path) {
	process_detail::Fd fd{::open(path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC)};
	if (fd.value < 0) { if (errno == ENOENT) return std::nullopt; process_detail::fail("open pidfile"); }
	struct stat st{};
	if (::fstat(fd.value, &st) || !S_ISREG(st.st_mode)) throw std::runtime_error("pidfile must be a regular file");
	if (::flock(fd.value, LOCK_EX | LOCK_NB) == 0) return std::nullopt;
	if (errno != EWOULDBLOCK && errno != EAGAIN) process_detail::fail("lock pidfile");
	char bytes[32];
	auto n = ::pread(fd.value, bytes, sizeof(bytes), 0);
	if (n <= 0) return std::nullopt; // startup may be writing its PID
	pid_t pid = 0;
	auto [end, error] = std::from_chars(bytes, bytes + n, pid);
	if (error != std::errc{} || end != bytes + n || pid <= 0) return std::nullopt;
	return pid;
}

inline ProcessLock::ProcessLock(const std::filesystem::path& path, bool system) {
	auto dir = path.parent_path();
	std::filesystem::create_directories(dir);
	struct stat st{};
	if (::lstat(dir.c_str(), &st) || !S_ISDIR(st.st_mode) || st.st_uid != ::geteuid()) {
		throw std::runtime_error("runtime directory must be owned by the current user and not be a symlink: " + dir.string());
	}
	if (::chmod(dir.c_str(), system ? 0755 : 0700)) process_detail::fail("chmod runtime directory");
	process_detail::Fd fd{::open(path.c_str(), O_CREAT | O_RDWR | O_NOFOLLOW | O_CLOEXEC, system ? 0644 : 0600)};
	if (fd.value < 0) process_detail::fail("open pidfile");
	if (::fstat(fd.value, &st) || !S_ISREG(st.st_mode) || st.st_uid != ::geteuid() || st.st_nlink != 1) {
		throw std::runtime_error("pidfile must be a singly linked regular file owned by the current user");
	}
	if (::flock(fd.value, LOCK_EX | LOCK_NB)) {
		if (errno == EWOULDBLOCK || errno == EAGAIN) return;
		process_detail::fail("lock pidfile");
	}
	auto pid = std::to_string(::getpid());
	if (::ftruncate(fd.value, 0)) process_detail::fail("truncate pidfile");
	if (::pwrite(fd.value, pid.data(), pid.size(), 0) != static_cast<ssize_t>(pid.size())) process_detail::fail("write pidfile");
	fd_ = fd.value;
	fd.value = -1;
}
inline ProcessLock::~ProcessLock() { if (fd_ >= 0) ::close(fd_); }

inline int run_daemon(bool detach, const std::filesystem::path& log_path, const std::function<int(const daemon_ready&)>& run,
               std::chrono::seconds startup_timeout) {
	if (!detach) return run([] {});
	int pipefd[2];
	if (::pipe(pipefd)) process_detail::fail("startup pipe");
	process_detail::Fd input{process_detail::high_fd(pipefd[0])}, output{process_detail::high_fd(pipefd[1])};
	if (input.value < 0 || output.value < 0) process_detail::fail("duplicate startup pipe");
	auto pid = ::fork();
	if (pid < 0) process_detail::fail("fork");
	if (pid == 0) {
		::close(input.value); input.value = -1;
		daemon_ready ready = [&] {
			if (output.value >= 0) {
				ssize_t n;
				do { n = ::write(output.value, "R", 1); } while (n < 0 && errno == EINTR);
				if (n != 1) process_detail::fail("report daemon readiness");
				::close(output.value); output.value = -1;
			}
		};
		try {
			if (::setsid() < 0) process_detail::fail("setsid");
			std::filesystem::create_directories(log_path.parent_path());
			process_detail::Fd log{process_detail::high_fd(::open(log_path.c_str(), O_CREAT | O_WRONLY | O_APPEND | O_NOFOLLOW | O_CLOEXEC, 0600))};
			if (log.value < 0) process_detail::fail("open daemon log");
			struct stat st{};
			if (::fstat(log.value, &st) || !S_ISREG(st.st_mode) || st.st_uid != ::geteuid() || st.st_nlink != 1) throw std::runtime_error("daemon log must be a singly linked regular file owned by the current user");
			if (::dup2(log.value, STDOUT_FILENO) < 0 || ::dup2(log.value, STDERR_FILENO) < 0) process_detail::fail("redirect daemon log");
			process_detail::Fd null{process_detail::high_fd(::open("/dev/null", O_RDONLY | O_CLOEXEC))};
			if (null.value < 0 || ::dup2(null.value, STDIN_FILENO) < 0) process_detail::fail("redirect stdin");
			auto result = run(ready);
			if (result != 0 && output.value >= 0) throw std::runtime_error("daemon startup failed");
			ready();
			::_exit(result);
		} catch (const std::exception& e) {
			if (output.value >= 0) {
				std::string message = "E";
				message += e.what();
				if (message.size() > 4096) message.resize(4096);
				(void)::write(output.value, message.data(), message.size());
			}
			::_exit(1);
		}
	}
	::close(output.value); output.value = -1;
	std::string error;
	auto deadline = std::chrono::steady_clock::now() + startup_timeout;
	for (;;) {
		auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
		pollfd pfd{input.value, POLLIN, 0};
		int result = remaining > 0 ? ::poll(&pfd, 1, static_cast<int>(remaining)) : 0;
		if (result < 0 && errno == EINTR) continue;
		if (result < 0) process_detail::fail("wait for daemon startup");
		if (result == 0) {
			::kill(pid, SIGTERM);
			throw std::runtime_error("daemon startup timed out");
		}
		char buffer[512];
		auto n = ::read(input.value, buffer, sizeof(buffer));
		if (n < 0 && errno == EINTR) continue;
		if (n < 0) process_detail::fail("read daemon startup");
		if (n == 0) break;
		error.append(buffer, n);
	}
	if (error != "R") {
		(void)::waitpid(pid, nullptr, 0);
		throw std::runtime_error(error.starts_with("E") ? error.substr(1) : "daemon exited without reporting readiness");
	}
	return 0;
}

}  // namespace task
