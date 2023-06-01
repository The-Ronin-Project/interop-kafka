package com.projectronin.interop.kafka.spring

import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationPropertiesScan(basePackageClasses = [KafkaConfig::class])
@ComponentScan("com.projectronin.interop.kafka")
class KafkaSpringConfig
