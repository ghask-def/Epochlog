plugins {
    id("epochlog.java-conventions")
}

dependencies {
    api(project(":common"))
    api(project(":log-storage"))
    api(project(":protocol"))
    testImplementation(project(":concurrency"))
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            limit {
                counter = "LINE"
                minimum = "0.80".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}
