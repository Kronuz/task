// The release half of layout_test: the same headers compiled with NDEBUG.
#ifndef NDEBUG
#define NDEBUG 1
#endif
#include "task.hh"

#include <cstddef>

std::size_t layout_release_size() { return sizeof(task::inline_scheduler); }
std::size_t layout_release_align() { return alignof(task::inline_scheduler); }
