// (A same-size member can hide in padding, so the awaiter also compares whether on_loop_ exists.)
// inline_scheduler and cancellable_asio_awaiter have one layout whatever NDEBUG is: two translation units, one compiled with
// NDEBUG and one without, must agree on its size and alignment (an ODR break otherwise).
#include <cstddef>
#include <cstdio>

std::size_t layout_release_size();
std::size_t layout_release_align();
std::size_t layout_debug_size();
std::size_t layout_debug_align();
#ifdef TASK_LAYOUT_ASIO
std::size_t layout_release_awaiter_size();
std::size_t layout_release_awaiter_align();
std::size_t layout_debug_awaiter_size();
std::size_t layout_debug_awaiter_align();
bool layout_release_awaiter_has_on_loop();
bool layout_debug_awaiter_has_on_loop();
#endif

int main() {
	bool ok = layout_release_size() == layout_debug_size() && layout_release_align() == layout_debug_align();
	std::printf("layout: release %zu/%zu, debug %zu/%zu: %s\n", layout_release_size(), layout_release_align(),
	            layout_debug_size(), layout_debug_align(), ok ? "ok" : "FAILED");
#ifdef TASK_LAYOUT_ASIO
	const bool aok = layout_release_awaiter_size() == layout_debug_awaiter_size() &&
	                 layout_release_awaiter_align() == layout_debug_awaiter_align() &&
	                 layout_release_awaiter_has_on_loop() == layout_debug_awaiter_has_on_loop();
	std::printf("layout awaiter: release %zu/%zu, debug %zu/%zu: %s\n", layout_release_awaiter_size(),
	            layout_release_awaiter_align(), layout_debug_awaiter_size(), layout_debug_awaiter_align(),
	            aok ? "ok" : "FAILED");
	ok = ok && aok;
#endif
	return ok ? 0 : 1;
}
