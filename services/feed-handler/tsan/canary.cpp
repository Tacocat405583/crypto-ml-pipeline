// A deliberate data race. The TSan image runs this before the real tests and
// requires TSan to report it: a sanitizer that can't see this race proves
// nothing by staying quiet about the feed handler.
#include <thread>

int main()
{
    int counter = 0;                        // shared, not atomic, not locked
    std::thread a([&] { for (int i = 0; i < 100000; ++i) ++counter; });
    std::thread b([&] { for (int i = 0; i < 100000; ++i) ++counter; });
    a.join();
    b.join();
    return counter == 0;                    // use the value so the loops survive -O1
}
