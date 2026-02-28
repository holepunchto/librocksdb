#if defined(__APPLE__)
#include "lock/apple.h"
#elif defined(__linux__)
#include "lock/linux.h"
#elif defined(_WIN32)
#include "lock/win32.h"
#endif
