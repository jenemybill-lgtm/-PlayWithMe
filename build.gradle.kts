plugins {
    kotlin("jvm") version "1.9.10"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    mavenCentral()
}

sourceSets {
    main {
        kotlin {
            setSrcDirs(listOf("."))
        }
    }
}

dependencies {
    implementation("io.ktor:ktor-server-core:2.3.5")
    implementation("io.ktor:ktor-server-netty:2.3.5")
    implementation("io.ktor:ktor-server-websockets:2.3.5")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.5")
    implementation("io.ktor:ktor-serialization-gson:2.3.5")
    implementation("com.google.code.gson:gson:2.10.1")
    implementation("org.mongodb:mongodb-driver-kotlin-coroutine:4.11.0")
    implementation("ch.qos.logback:logback-classic:1.4.11")
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "ServerKt"
    }
}
