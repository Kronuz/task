// inline_scheduler has one layout whatever NDEBUG is: two translation units, one compiled with
// NDEBUG and one without, must agree on its size and alignment (an ODR break otherwise).
#include <cstddef>
#include <cstdio>

std::size_t layout_release_size();
std::size_t layout_release_align();
std::size_t layout_debug_size();
std::size_t layout_debug_align();

int main() {
	const bool ok = layout_release_size() == layout_debug_size() && layout_release_align() == layout_debug_align();
	std::printf("layout: release %zu/%zu, debug %zu/%zu: %s\n", layout_release_size(), layout_release_align(),
	            layout_debug_size(), layout_debug_align(), ok ? "ok" : "FAILED");
	return ok ? 0 : 1;
}
