#if defined(__APPLE__)
#include "xattr/apple.h"
#elif defined(__linux__)
#include "xattr/linux.h"
#elif defined(_WIN32)
#include "xattr/win32.h"
#endif
