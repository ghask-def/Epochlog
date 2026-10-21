plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":common"))
    implementation(project(":concurrency"))
}
