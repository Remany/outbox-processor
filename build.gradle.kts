plugins {
    id("java-library")
    id("io.spring.dependency-management") version "1.1.7"
    `maven-publish`
}

group = "ru.romanov"
version = "1.0.21"

repositories {
    mavenLocal()
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    api("org.springframework.boot:spring-boot-starter:3.5.6")
    api("org.springframework.boot:spring-boot-starter-data-jdbc:3.5.6")
    api("org.springframework.boot:spring-boot-starter-json:3.5.6")
    api("org.springframework.kafka:spring-kafka:3.2.4")
    api("io.micrometer:micrometer-core:1.14.3")
    api("io.micrometer:micrometer-registry-prometheus:1.14.3")
    compileOnly("org.projectlombok:lombok:1.18.34")
    runtimeOnly("org.postgresql:postgresql:42.7.3")
    annotationProcessor("org.projectlombok:lombok:1.18.34")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:3.5.6")
    testImplementation("org.springframework.boot:spring-boot-starter-test:3.5.6")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.2")
    testCompileOnly("org.projectlombok:lombok:1.18.34")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.34")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<AbstractPublishToMaven> {
    dependsOn(tasks.named("jar"))
}

/*
──────────────────────────────────────────────────────
============== Resolve NEXUS credentials ==============
──────────────────────────────────────────────────────
*/

file(".env").takeIf { it.exists() }?.readLines()?.forEach {
    val (k, v) = it.split("=", limit = 2)
    System.setProperty(k.trim(), v.trim())
    logger.lifecycle("${k.trim()}=${v.trim()}")
}

val nexusUrl = System.getenv("NEXUS_URL") ?: System.getProperty("NEXUS_URL")
val nexusUser = System.getenv("NEXUS_USERNAME") ?: System.getProperty("NEXUS_USERNAME")
val nexusPassword = System.getenv("NEXUS_PASSWORD") ?: System.getProperty("NEXUS_PASSWORD")

if (nexusUrl.isNullOrBlank() || nexusUser.isNullOrBlank() || nexusPassword.isNullOrBlank()) {
    throw GradleException(
        "NEXUS details are not set. Create a .env file with correct properties: " + "NEXUS_URL, NEXUS_USERNAME, NEXUS_PASSWORD"
    )
}

/*
──────────────────────────────────────────────────────
============== Nexus Publishing ==============
──────────────────────────────────────────────────────
*/

publishing {
    publications {
        val jarBaseName = name
        val jarFile = file("build/libs").listFiles()
            ?.firstOrNull { it.name.contains(name) && (it.extension == "jar" || it.extension == "zip") }

        if (jarFile != null) {
            logger.lifecycle("publishing: ${jarFile.name}")

            create<MavenPublication>("publish${name.replaceFirstChar(Char::uppercase)}Jar") {
                artifact(jarFile)
                groupId = "ru.romanov"
                artifactId = jarBaseName
                version = "1.0.21"

                pom.withXml {
                    asNode().children().clear()

                    asNode().appendNode("modelVersion", "4.0.0")
                    asNode().appendNode("groupId", project.group)
                    asNode().appendNode("artifactId", project.name)
                    asNode().appendNode("version", project.version)
                    asNode().appendNode("packaging", "jar")

                    val dependenciesNode = asNode().appendNode("dependencies")

                    configurations.api.get().allDependencies.forEach { dep ->
                        if (dep is ModuleDependency && dep.group != null) {
                            val dependencyNode = dependenciesNode.appendNode("dependency")
                            dependencyNode.appendNode("groupId", dep.group)
                            dependencyNode.appendNode("artifactId", dep.name)
                            dependencyNode.appendNode("version", getResolvedVersion(dep))
                            dependencyNode.appendNode("scope", "compile")
                        }
                    }

                    configurations.runtimeOnly.get().allDependencies.forEach { dep ->
                        if (dep is ModuleDependency && dep.group != null) {
                            val dependencyNode = dependenciesNode.appendNode("dependency")
                            dependencyNode.appendNode("groupId", dep.group)
                            dependencyNode.appendNode("artifactId", dep.name)
                            dependencyNode.appendNode("version", getResolvedVersion(dep))
                            dependencyNode.appendNode("scope", "runtime")
                        }
                    }

                    configurations.compileOnly.get().allDependencies.forEach { dep ->
                        if (dep is ModuleDependency && dep.group != null) {
                            val dependencyNode = dependenciesNode.appendNode("dependency")
                            dependencyNode.appendNode("groupId", dep.group)
                            dependencyNode.appendNode("artifactId", dep.name)
                            dependencyNode.appendNode("version", getResolvedVersion(dep))
                            dependencyNode.appendNode("scope", "provided")
                        }
                    }
                }
            }
        }

    }

    repositories {
        maven {
            name = "nexus"
            url = uri(nexusUrl)
            isAllowInsecureProtocol = true
            credentials {
                username = nexusUser
                password = nexusPassword
            }
        }
    }
}

fun getResolvedVersion(dep: ModuleDependency): String {
    return when {
        dep.version != null -> dep.version!!
        dep.name.contains("spring-boot") -> "3.5.6"
        dep.name.contains("lombok") -> "1.18.34"
        dep.name.contains("postgresql") -> "42.7.3"
        dep.name.contains("kafka") -> "3.2.4"
        dep.name.contains("micrometer") -> "1.14.3"
        else -> "REQUIRED"
    }
}