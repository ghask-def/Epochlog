plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":common"))
    api(project(":protocol"))
    implementation(project(":concurrency"))
    testImplementation(project(":broker"))
    testImplementation(project(":client"))
}
