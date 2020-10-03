package com.hazelcast.jet.demos.traintrack;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.ClasspathYamlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;

/**
 * <p>"<i>temporary code</i>"
 * </p>
 * <p>This class defines Spring beans to create a single Hazelcast Jet instance
 * in this JVM, and to expose the IMDG instance embedded in the Jet instance.
 * </p>
 * <p>There is a pull request (<a href="https://github.com/spring-projects/spring-boot/issues/8863">Issue 8863</a>)
 * for Spring Boot to do this for us automatically. Once this is merged,
 * the onerous burden of writing the few lines of code below can be eliminated.
 * </p>
 */
@Configuration
public class ApplicationConfig {

    @Bean
    public Config config() {
        return new ClasspathYamlConfig("hazelcast.yaml");
    }
        
    @Bean
    public JetInstance jetInstance(Config config) {
        JetConfig jetConfig = new JetConfig().setHazelcastConfig(config);
        return Jet.newJetInstance(jetConfig);
    }

    @Bean
    public HazelcastInstance hazelcastInstance(JetInstance jetInstance) {
        return jetInstance.getHazelcastInstance();
    }

}
