plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.10"
    id("io.ktor.plugin") version "2.3.5"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.example"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.ktor:ktor-server-core:2.3.5")
    implementation("io.ktor:ktor-server-netty:2.3.5")
    implementation("io.ktor:ktor-server-websockets:2.3.5")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.5")
    implementation("io.ktor:ktor-serialization-gson:2.3.5")
    implementation("com.google.code.gson:gson:2.10.1")
    
    // MongoDB Driver - Απαραίτητο για τους φίλους!
    implementation("org.mongodb:mongodb-driver-kotlin-coroutine:4.11.0")
    
    implementation("ch.qos.logback:logback-classic:1.4.11")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("com.example.ServerKt") 
}
}
tasks.getByName<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("server")
    archiveClassifier.set("all")
    archiveVersion.set("")
}
