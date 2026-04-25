plugins {
    kotlin("jvm") version "2.1.21"
    kotlin("plugin.serialization") version "2.1.21"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.2"
    // detekt 1.23.x breaks on JDK 25; pending 2.0 GA.
    // id("io.gitlab.arturbosch.detekt") version "1.23.7"
    application
}

group = "io.conduktor.kri"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

kotlin {
    jvmToolchain(21)
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

val kafkaVersion = "3.9.0"
val confluentVersion = "7.8.0"
val roaringVersion = "1.3.0"
val hash4jVersion = "0.20.0"
val javalinVersion = "6.4.0"
val jacksonVersion = "2.18.2"
val kamlVersion = "0.72.0"
val slf4jVersion = "2.0.16"

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")

    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    implementation("org.apache.avro:avro:1.12.0")

    implementation("org.roaringbitmap:RoaringBitmap:$roaringVersion")
    implementation("com.dynatrace.hash4j:hash4j:$hash4jVersion")

    implementation("io.javalin:javalin:$javalinVersion")

    implementation("com.charleskorn.kaml:kaml:$kamlVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    implementation("com.networknt:json-schema-validator:1.5.3")

    implementation("io.micrometer:micrometer-registry-prometheus:1.14.2")

    implementation("com.github.luben:zstd-jni:1.5.6-8")

    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    implementation("ch.qos.logback:logback-classic:1.5.12")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.21.3"))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
}

application {
    mainClass.set("io.conduktor.kri.MainKt")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions {
        freeCompilerArgs.addAll("-opt-in=kotlin.RequiresOptIn")
    }
}

ktlint {
    version.set("1.4.1")
    verbose.set(true)
    outputToConsole.set(true)
    ignoreFailures.set(false)
    enableExperimentalRules.set(false)
    filter {
        exclude("**/build/**")
        exclude("**/generated/**")
    }
}

tasks.named("check") {
    dependsOn("ktlintCheck")
}

tasks.register("installGitHooks") {
    group = "verification"
    description = "Installs the pre-commit hook into .git/hooks"
    doLast {
        val src = file("scripts/git-hooks/pre-commit")
        val dst = file(".git/hooks/pre-commit")
        if (!src.exists()) throw GradleException("missing $src")
        src.copyTo(dst, overwrite = true)
        dst.setExecutable(true)
        println("Installed $dst")
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
    maxHeapSize = "2g"
    // Testcontainers: propagate DOCKER_HOST; Docker Desktop advertises its actual CLI socket here.
    val dockerHost =
        System.getenv("DOCKER_HOST")
            ?: System.getProperty("user.home").let { "unix://$it/Library/Containers/com.docker.docker/Data/docker.raw.sock" }
    environment("DOCKER_HOST", dockerHost)
    environment("DOCKER_API_VERSION", System.getenv("DOCKER_API_VERSION") ?: "1.48")
    environment("TESTCONTAINERS_RYUK_DISABLED", System.getenv("TESTCONTAINERS_RYUK_DISABLED") ?: "true")
    systemProperty("api.version", "1.48")
    systemProperty("DOCKER_API_VERSION", "1.48")
}

tasks.register<JavaExec>("runIndexer") {
    group = "application"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("io.conduktor.kri.MainKt")
    val configPath = project.findProperty("config") as String? ?: "indexer.yaml"
    args("--config", configPath)
}

tasks.register<JavaExec>("runBench") {
    group = "application"
    description = "In-process ingest + query benchmark. Pass args via -Pargs='--events 20000000 --buckets 6' or BENCH_ARGS env"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("io.conduktor.kri.bench.BenchKt")
    jvmArgs = listOf("-Xms2g", "-Xmx6g", "-XX:+UseG1GC")
    val raw = (project.findProperty("args") as String?) ?: System.getenv("BENCH_ARGS")
    val extra = raw?.split(" ")?.filter { it.isNotBlank() } ?: emptyList()
    args(extra)
    standardInput = System.`in`
}

tasks.register<Jar>("benchJar") {
    group = "application"
    description = "Fat JAR for the bench server (used by Docker)"
    archiveClassifier.set("bench")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest { attributes["Main-Class"] = "io.conduktor.kri.bench.BenchKt" }
    from(sourceSets["main"].output)
    from(
        configurations.runtimeClasspath
            .get()
            .map { if (it.isDirectory) it else zipTree(it) },
    ) {
        exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
        exclude("META-INF/LICENSE*", "META-INF/NOTICE*", "META-INF/DEPENDENCIES")
    }
}
