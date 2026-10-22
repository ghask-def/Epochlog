plugins {
    id("epochlog.java-conventions")
}

dependencies {
    implementation(project(":concurrency"))
    implementation(project(":log-storage"))
    implementation(project(":clock"))
    implementation(project(":replication"))
    implementation(libs.jcstress.core)
    annotationProcessor(libs.jcstress.core)
}

// jcstress generates code that trips -Xlint, so relax warnings for this module only.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs = listOf("-Xlint:none")
}

tasks.register<JavaExec>("jcstress") {
    group = "verification"
    description = "Runs the jcstress concurrency tests. Pass -Pjcstress.mode=tough for a long run."
    dependsOn(tasks.classes)
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass = "org.openjdk.jcstress.Main"
    val mode = providers.gradleProperty("jcstress.mode").getOrElse("quick")
    val filter = providers.gradleProperty("jcstress.tests").getOrElse("io.epochlog.stress")
    args("-m", mode, "-t", filter, "-r", layout.buildDirectory.dir("jcstress-results").get().asFile.path)
    workingDir = layout.buildDirectory.get().asFile
}
