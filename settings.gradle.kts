plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "epochlog"

include(
    "common",
    "log-storage",
    "concurrency",
    "clock",
    "protocol",
    "broker",
    "replication",
    "controller",
    "client",
    "cli",
    "stress-tests",
    "benchmarks",
    "chaos-tests",
    "deploy",
)
