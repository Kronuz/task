// The Debug half of layout_test: the same headers compiled without NDEBUG.
#undef NDEBUG
#include "task.hh"
#ifdef TASK_LAYOUT_ASIO
// The asio headers first, so only task_asio.hh is opened with private redefined (the probe reads on_loop_).
#include <asio/any_io_executor.hpp>
#include <asio/bind_cancellation_slot.hpp>
#include <asio/cancellation_signal.hpp>
#include <asio/error.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#define private public
#include "task_asio.hh"
#undef private
#endif

#include <cstddef>

std::size_t layout_debug_size() { return sizeof(task::inline_scheduler); }
std::size_t layout_debug_align() { return alignof(task::inline_scheduler); }

#ifdef TASK_LAYOUT_ASIO
template <typename T>
constexpr bool has_on_loop = requires { &T::on_loop_; };
using awaiter = task::detail::cancellable_asio_awaiter<void(asio::error_code, std::size_t), void (*)()>;
std::size_t layout_debug_awaiter_size() { return sizeof(awaiter); }
std::size_t layout_debug_awaiter_align() { return alignof(awaiter); }
bool layout_debug_awaiter_has_on_loop() { return has_on_loop<awaiter>; }
#endif
