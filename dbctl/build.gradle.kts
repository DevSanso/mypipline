import org.gradle.kotlin.dsl.get

plugins {
    application
    kotlin("jvm") version "2.2.21"
}

group = "com.github.devsanso.mypipline"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.jetbrains.exposed:exposed-core:1.0.0-rc-4")
    implementation("org.jetbrains.exposed:exposed-migration-core:1.0.0-rc-4")
    runtimeOnly("org.jetbrains.exposed:exposed-jdbc:1.0.0-rc-4")
    runtimeOnly("org.jetbrains.exposed:exposed-dao:1.0.0-rc-4")
    implementation("org.jetbrains.exposed:exposed-migration-jdbc:1.0.0-rc-4")
    implementation("com.fasterxml.jackson.core:jackson-core:2.20.1")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-toml:2.21.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.20.1")
    implementation("org.postgresql:postgresql:42.7.9")
    implementation("org.slf4j:slf4j-simple:2.0.16")

}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("com.github.devsanso.mypipline.MainKt")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = application.mainClass.get()
    }
}