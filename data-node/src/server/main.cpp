//
// Created by Felix Zhang on 2025-09-03.
//

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>

#include "backend/backend.h"
#include "frontend/listener.h"
#include "frontend/tracker.h"

int main(int argc, char* argv[]) {
    assert(argc == 1 || argc == 2);

    bool resilient = false;
    if (argc == 2 && strcmp(argv[1], "--fault-tolerant") == 0) {
        resilient = true;
    }

    std::cout << "Starting disaggregated backend..." << std::endl;
    if (fdl::backend().start(resilient) != EXIT_SUCCESS) {
        fdl::backend().stop();
        return EXIT_FAILURE;
    }

    fdl::Listener::instance().run();

    // Reached only if the listener (unexpectedly) exits
    fdl::backend().stop();
    return EXIT_SUCCESS;
}
