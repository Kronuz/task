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

// task's process.hh: run_daemon, ProcessLock and locked_pid.
//   ready      a detached child's ready() releases the parent with 0, before the child ends
//   detach     the child is a session leader with stdout and stderr on the log
//   throws     a throw before ready() reaches the parent with its message
//   failed     a nonzero result or an exit before ready() is reported by the parent
//   timeout    a child that never reports is SIGTERMed and the parent throws
//   in-place   detach false runs in place with a no-op ready
//   lock       a held path refuses a second lock; the first writes its PID
//   locked-pid the holder's PID while held; none for a stale, missing or half-written file
//   unsafe     a symlinked pidfile and a foreign runtime directory are refused
#include "process.hh"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include <sys/stat.h>

#define CHECK(c)                                                              \
	do {                                                                      \
		if (!(c)) {                                                           \
			std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); \
			std::exit(1);                                                     \
		}                                                                     \
	} while (0)

namespace {
using namespace std::chrono_literals;
namespace fs = std::filesystem;
using clock_type = std::chrono::steady_clock;

fs::path g_dir;

std::string slurp(const fs::path& p) {
	std::ifstream in(p);
	std::stringstream s;
	s << in.rdbuf();
	return s.str();
}

// Waits up to 5 s for the file to hold at least `want`.
std::string wait_for(const fs::path& p, const std::string& want) {
	for (int i = 0; i < 500; ++i) {
		auto s = slurp(p);
		if (s.find(want) != std::string::npos) { return s; }
		std::this_thread::sleep_for(10ms);
	}
	return slurp(p);
}

// The message run_daemon throws, or "" when it returns.
template <class F>
std::string thrown(F&& f) {
	try { f(); } catch (const std::exception& e) { return e.what(); }
	return "";
}

void ready_releases() {
	auto log = g_dir / "ready.log";
	auto start = clock_type::now();
	int r = task::run_daemon(true, log, [](const task::daemon_ready& ready) {
		ready();
		std::this_thread::sleep_for(1s);
		std::printf("done\n");
		std::fflush(stdout);
		return 0;
	});
	auto took = clock_type::now() - start;
	CHECK(r == 0);
	CHECK(took < 800ms);  // released at ready(), not at the child's end
	CHECK(wait_for(log, "done") == "done\n");
}

void detach() {
	auto log = g_dir / "detach.log";
	int r = task::run_daemon(true, log, [](const task::daemon_ready& ready) {
		std::printf("leader=%d\n", static_cast<int>(::getsid(0) == ::getpid()));
		std::fprintf(stderr, "err\n");
		char c;
		std::printf("stdin=%zd\n", ::read(STDIN_FILENO, &c, 1));
		std::fflush(stdout);
		ready();
		return 0;
	});
	CHECK(r == 0);
	auto s = wait_for(log, "err\n");
	CHECK(s.find("leader=1\n") != std::string::npos);
	CHECK(s.find("stdin=0\n") != std::string::npos);
	CHECK(s.find("err\n") != std::string::npos);
	struct stat st{};
	CHECK(::stat(log.c_str(), &st) == 0 && (st.st_mode & 0777) == 0600);
}

void throws() {
	auto log = g_dir / "throws.log";
	CHECK(thrown([&] {
		task::run_daemon(true, log, [](const task::daemon_ready&) -> int { throw std::runtime_error("no port"); });
	}) == "no port");
	// A throw after ready() is the child's: the parent has already returned 0.
	CHECK(task::run_daemon(true, log, [](const task::daemon_ready& ready) -> int {
		ready();
		throw std::runtime_error("late");
	}) == 0);
}

void failed() {
	auto log = g_dir / "failed.log";
	CHECK(thrown([&] {
		task::run_daemon(true, log, [](const task::daemon_ready&) { return 3; });
	}) == "daemon startup failed");
	CHECK(thrown([&] {
		task::run_daemon(true, log, [](const task::daemon_ready&) -> int { ::_exit(5); });
	}) == "daemon exited without reporting readiness");
	// A zero result without an explicit ready() still reports readiness.
	CHECK(task::run_daemon(true, log, [](const task::daemon_ready&) { return 0; }) == 0);
	// A log path that cannot be opened is a startup error.
	auto bad = g_dir / "failed.dir";
	fs::create_directory(bad);
	CHECK(!thrown([&] { task::run_daemon(true, bad, [](const task::daemon_ready&) { return 0; }); }).empty());
}

void timeout() {
	auto log = g_dir / "timeout.log";
	auto pidfile = g_dir / "timeout.pid";
	auto start = clock_type::now();
	auto msg = thrown([&] {
		task::run_daemon(true, log, [&](const task::daemon_ready&) {
			std::ofstream(pidfile) << ::getpid() << std::flush;
			std::this_thread::sleep_for(30s);
			return 0;
		}, 1s);
	});
	auto took = clock_type::now() - start;
	CHECK(msg == "daemon startup timed out");
	CHECK(took >= 900ms && took < 5s);
	std::string text;
	for (int i = 0; i < 500 && text.empty(); ++i) { text = slurp(pidfile); std::this_thread::sleep_for(10ms); }
	CHECK(!text.empty());
	int st = 0;
	CHECK(::waitpid(std::stoi(text), &st, 0) > 0);  // the child is ours
	CHECK(WIFSIGNALED(st) && WTERMSIG(st) == SIGTERM);
}

void in_place() {
	int calls = 0;
	int r = task::run_daemon(false, g_dir / "none.log", [&](const task::daemon_ready& ready) {
		ready();
		++calls;
		return 7;
	});
	CHECK(r == 7 && calls == 1);
	CHECK(!fs::exists(g_dir / "none.log"));
}

void lock() {
	auto path = g_dir / "run" / "d.pid";
	{
		task::ProcessLock a(path, false);
		CHECK(a.acquired());
		CHECK(slurp(path) == std::to_string(::getpid()));
		struct stat st{};
		CHECK(::stat(path.c_str(), &st) == 0 && (st.st_mode & 0777) == 0600);
		CHECK(::stat(path.parent_path().c_str(), &st) == 0 && (st.st_mode & 0777) == 0700);
		task::ProcessLock b(path, false);
		CHECK(!b.acquired());
		CHECK(slurp(path) == std::to_string(::getpid()));  // the refused lock leaves the holder's PID
	}
	task::ProcessLock c(path, true);  // released with the holder
	CHECK(c.acquired());
	struct stat st{};
	CHECK(::stat(path.parent_path().c_str(), &st) == 0 && (st.st_mode & 0777) == 0755);
}

void locked_pid() {
	auto path = g_dir / "pid" / "d.pid";
	CHECK(!task::locked_pid(path));  // missing
	// A child holds the lock; the parent sees the child's PID.
	int up[2], down[2];
	CHECK(::pipe(up) == 0 && ::pipe(down) == 0);
	pid_t child = ::fork();
	CHECK(child >= 0);
	if (child == 0) {
		task::ProcessLock l(path, false);
		char c = l.acquired() ? 'Y' : 'N';
		(void)::write(up[1], &c, 1);
		(void)::read(down[0], &c, 1);  // hold until released
		::_exit(0);
	}
	char c = 0;
	CHECK(::read(up[0], &c, 1) == 1 && c == 'Y');
	auto held = task::locked_pid(path);
	CHECK(held && *held == child);
	CHECK(*held != ::getpid());
	(void)::write(down[1], "x", 1);
	int st = 0;
	CHECK(::waitpid(child, &st, 0) == child && WIFEXITED(st) && WEXITSTATUS(st) == 0);
	// The file still names the dead child, but nothing holds it: a stale file names no process.
	CHECK(slurp(path) == std::to_string(child));
	CHECK(!task::locked_pid(path));
	// A held lock with an unwritten or garbled PID names no process either.
	{
		task::ProcessLock l(path, false);
		CHECK(l.acquired());
		CHECK(task::locked_pid(path) == ::getpid());
		CHECK(::truncate(path.c_str(), 0) == 0);
		CHECK(!task::locked_pid(path));
		std::ofstream(path, std::ios::trunc) << "12ab";
		CHECK(!task::locked_pid(path));
		std::ofstream(path, std::ios::trunc) << "0";
		CHECK(!task::locked_pid(path));
	}
}

void unsafe() {
	auto dir = g_dir / "unsafe";
	fs::create_directories(dir);
	auto target = dir / "target";
	std::ofstream(target) << "1";
	auto link = dir / "link.pid";
	fs::create_symlink(target, link);
	CHECK(!thrown([&] { task::ProcessLock l(link, false); }).empty());
	CHECK(!thrown([&] { task::locked_pid(link); }).empty());
	auto hard = dir / "hard.pid";
	{ task::ProcessLock l(hard, false); }
	fs::create_hard_link(hard, dir / "hard2.pid");
	CHECK(!thrown([&] { task::ProcessLock l(hard, false); }).empty());
	auto sub = g_dir / "realdir";
	fs::create_directory(sub);
	auto dlink = g_dir / "dirlink";
	fs::create_directory_symlink(sub, dlink);
	CHECK(!thrown([&] { task::ProcessLock l(dlink / "d.pid", false); }).empty());
}
}  // namespace

int main(int argc, char** argv) {
	const std::string_view only = argc > 1 ? argv[1] : "all";
	// POSIX fork tests first: they run before any thread of ours exists.
	char tmpl[] = "process_test.XXXXXX";
	CHECK(::mkdtemp(tmpl));
	g_dir = fs::absolute(tmpl);
	if (only == "all" || only == "ready") { ready_releases(); }
	if (only == "all" || only == "detach") { detach(); }
	if (only == "all" || only == "throws") { throws(); }
	if (only == "all" || only == "failed") { failed(); }
	if (only == "all" || only == "timeout") { timeout(); }
	if (only == "all" || only == "in-place") { in_place(); }
	if (only == "all" || only == "lock") { lock(); }
	if (only == "all" || only == "locked-pid") { locked_pid(); }
	if (only == "all" || only == "unsafe") { unsafe(); }
	fs::remove_all(g_dir);
	std::printf("process_test: ok\n");
	return 0;
}
