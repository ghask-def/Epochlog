plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":common"))
    api(project(":log-storage"))
    api(project(":protocol"))
    api(project(":concurrency"))
    api(project(":clock"))
    testImplementation(project(":client"))
}
