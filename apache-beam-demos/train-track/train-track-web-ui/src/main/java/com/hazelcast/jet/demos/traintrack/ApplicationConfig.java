package com.hazelcast.jet.demos.traintrack;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

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
    public ClientConfig clientConfig() throws Exception {
        return new YamlClientConfigBuilder("hazelcast-client.yaml").build();
    }
        
    @Bean
    public JetInstance jetInstance(ClientConfig clientConfig) {
        return Jet.newJetClient(clientConfig);
    }

    @Bean
    public HazelcastInstance hazelcastInstance(JetInstance jetInstance) {
        return jetInstance.getHazelcastInstance();
    }

}
