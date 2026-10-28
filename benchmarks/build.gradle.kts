plugins {
    id("epochlog.java-conventions")
}

dependencies {
    implementation(project(":log-storage"))
    implementation(project(":concurrency"))
    implementation(project(":broker"))
    implementation(project(":controller"))
    implementation(project(":client"))
    implementation(libs.jmh.core)
    annotationProcessor(libs.jmh.processor)
    runtimeOnly(libs.logback.classic)
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs = listOf("-Xlint:none")
}

tasks.register<JavaExec>("jmh") {
    group = "benchmark"
    description = "Runs JMH benchmarks. Pass -Pjmh.args='<regex> -f 1' to narrow the run."
    dependsOn(tasks.classes)
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "org.openjdk.jmh.Main"
    val extra = providers.gradleProperty("jmh.args").getOrElse("")
    args(extra.split(" ").filter { it.isNotBlank() })
}

tasks.register<JavaExec>("loadgen") {
    group = "benchmark"
    description = "End-to-end load test against an in-process 3-broker cluster. -Ploadgen.args='seconds=10 ...'"
    dependsOn(tasks.classes)
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "io.epochlog.bench.LoadGenerator"
    maxHeapSize = "4g"
    jvmArgs("-XX:+UseZGC", "-XX:+ZGenerational")
    val extra = providers.gradleProperty("loadgen.args").getOrElse("")
    args(extra.split(" ").filter { it.isNotBlank() })
}
