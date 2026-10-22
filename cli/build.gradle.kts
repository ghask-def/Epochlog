plugins {
    id("epochlog.java-conventions")
    application
}

dependencies {
    implementation(project(":client"))
    implementation(libs.picocli)
    annotationProcessor("info.picocli:picocli-codegen:4.7.6")
    runtimeOnly(libs.logback.classic)
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Aproject=${project.group}/${project.name}")
}

application {
    mainClass = "io.epochlog.cli.EpochlogCli"
    applicationName = "epochlog"
}

// One launcher per tool, so the scripts read like the commands in the README.
val tools = mapOf(
    "epochlog-produce" to "io.epochlog.cli.ProduceCommand",
    "epochlog-consume" to "io.epochlog.cli.ConsumeCommand",
    "epochlog-admin" to "io.epochlog.cli.AdminCommand",
)
tools.forEach { (scriptName, mainClassName) ->
    val task = tasks.register<CreateStartScripts>("startScripts-$scriptName") {
        applicationName = scriptName
        mainClass = mainClassName
        outputDir = layout.buildDirectory.dir("tool-scripts/$scriptName").get().asFile
        classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
    }
    distributions.main.get().contents.from(task) { into("bin") }
}
