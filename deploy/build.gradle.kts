plugins {
    base
    application
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation(project(":broker"))
    implementation(project(":controller"))
    implementation(project(":cli"))
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass = "io.epochlog.broker.BrokerMain"
    applicationName = "epochlog-broker"
    applicationDefaultJvmArgs = listOf("-XX:+UseZGC", "-XX:+ZGenerational")
}

val launchers = mapOf(
    "epochlog-controller" to "io.epochlog.controller.ControllerMain",
    "epochlog-produce" to "io.epochlog.cli.ProduceCommand",
    "epochlog-consume" to "io.epochlog.cli.ConsumeCommand",
    "epochlog-admin" to "io.epochlog.cli.AdminCommand",
)
launchers.forEach { (scriptName, mainClassName) ->
    val task = tasks.register<CreateStartScripts>("startScripts-$scriptName") {
        applicationName = scriptName
        mainClass = mainClassName
        outputDir = layout.buildDirectory.dir("launchers/$scriptName").get().asFile
        classpath = tasks.named<CreateStartScripts>("startScripts").get().classpath
        if (scriptName != "epochlog-controller") {
            defaultJvmOpts = listOf("-Dlogback.configurationFile=logback-cli.xml")
        } else {
            defaultJvmOpts = listOf("-XX:+UseZGC", "-XX:+ZGenerational")
        }
    }
    distributions.main.get().contents.from(task) { into("bin") }
}

distributions.main.get().distributionBaseName = "epochlog"
