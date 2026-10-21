plugins {
    id("epochlog.java-conventions")
}

dependencies {
    implementation(project(":log-storage"))
    implementation(project(":concurrency"))
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
