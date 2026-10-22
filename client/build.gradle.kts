plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":common"))
    api(project(":protocol"))
    implementation(project(":clock"))
    testImplementation(project(":broker"))
}
