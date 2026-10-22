plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":common"))
    api(project(":log-storage"))
    api(project(":protocol"))
}
