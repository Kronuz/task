// The Debug half of layout_test: the same headers compiled without NDEBUG.
#undef NDEBUG
#include "task.hh"

#include <cstddef>

std::size_t layout_debug_size() { return sizeof(task::inline_scheduler); }
std::size_t layout_debug_align() { return alignof(task::inline_scheduler); }
