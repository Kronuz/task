// The release half of layout_test: the same headers compiled with NDEBUG.
#ifndef NDEBUG
#define NDEBUG 1
#endif
#include "task.hh"
#ifdef TASK_LAYOUT_ASIO
// Expose private members so the probe can tell whether on_loop_ exists.
#define private public
#include "task_asio.hh"
#undef private
#endif

#include <cstddef>

std::size_t layout_release_size() { return sizeof(task::inline_scheduler); }
std::size_t layout_release_align() { return alignof(task::inline_scheduler); }

#ifdef TASK_LAYOUT_ASIO
template <typename T>
constexpr bool has_on_loop = requires { &T::on_loop_; };
using awaiter = task::detail::cancellable_asio_awaiter<void(asio::error_code, std::size_t), void (*)()>;
std::size_t layout_release_awaiter_size() { return sizeof(awaiter); }
std::size_t layout_release_awaiter_align() { return alignof(awaiter); }
bool layout_release_awaiter_has_on_loop() { return has_on_loop<awaiter>; }
#endif
