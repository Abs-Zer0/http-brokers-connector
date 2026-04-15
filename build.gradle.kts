plugins {
    id("io.micronaut.application") version "4.6.2"
    id("com.gradleup.shadow") version "8.3.9"
    id("io.micronaut.test-resources") version "4.6.2"
    id("io.micronaut.aot") version "4.6.2"
}

version = "0.1"
group = "abs.zer0"



repositories {
    mavenCentral()
}

dependencies {
    annotationProcessor("io.micronaut:micronaut-http-validation")
    annotationProcessor("io.micronaut.serde:micronaut-serde-processor")

    //implementation("io.micronaut.kafka:micronaut-kafka")
    implementation("io.micronaut:micronaut-management")
    implementation("io.micronaut.reactor:micronaut-reactor")
    implementation("io.micronaut.serde:micronaut-serde-jackson")

    runtimeOnly("ch.qos.logback:logback-classic")

    testImplementation("io.micronaut:micronaut-http-client")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}



application {
    mainClass = "abs.zer0.Application"
}

java {
    sourceCompatibility = JavaVersion.toVersion("25")
    targetCompatibility = JavaVersion.toVersion("25")
}




graalvmNative.toolchainDetection = false





micronaut {
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("abs.zer0.*")
    }
    testResources {
        sharedServer = true
    }
    aot {
        // Please review carefully the optimizations enabled below
        // Check https://micronaut-projects.github.io/micronaut-aot/latest/guide/ for more details
        optimizeServiceLoading = false
        convertYamlToJava = false
        precomputeOperations = true
        cacheEnvironment = true
        optimizeClassLoading = true
        deduceEnvironment = true
        optimizeNetty = true
        replaceLogbackXml = true
    }

}


tasks.named<io.micronaut.gradle.docker.NativeImageDockerfile>("dockerfileNative") {
    jdkVersion = "25"
}







