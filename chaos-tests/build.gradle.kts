plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":broker"))
    api(project(":controller"))
    api(project(":client"))
}

tasks.test {
    maxParallelForks = 1
    // Scenarios start whole clusters; give them room and keep the logs readable.
    maxHeapSize = "2g"
}
