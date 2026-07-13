plugins {
    java
    id("com.gradleup.shadow") version "8.3.5"
}

repositories {
    mavenCentral()
}

group = "kz.ffloppa"
version = "1.0-SNAPSHOT"

dependencies {
    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    implementation("com.discord4j:discord4j-core:3.3.2")
    implementation("ch.qos.logback:logback-classic:1.5.6")
    implementation("org.postgresql:postgresql:42.7.5")
    implementation("io.github.cdimascio:dotenv-java:3.0.0")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "kz.ffloppa.Main"
    }

    dependsOn("shadowJar")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.named<Jar>("shadowJar") {
    manifest {
        attributes("Main-Class" to "kz.ffloppa.Main")
    }
}
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}
tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()

    configurations = listOf(project.configurations.runtimeClasspath.get())
}
tasks.test {
    useJUnitPlatform()
}