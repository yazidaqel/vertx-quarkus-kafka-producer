package com.example;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
public class KafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    @Inject
    private Vertx vertx;

    private String deploymentId;



    void onStart(@Observes StartupEvent ev) {
        LOGGER.info("onStart");
        startKafkaConnection();
    }

    private void startKafkaConnection() {
        KafkaProducerVerticle producerVerticle = new KafkaProducerVerticle();
        vertx.deployVerticle(producerVerticle, handler ->{
            if(handler.succeeded()){
                deploymentId = handler.result();
            }
        });
    }

    void onStop(@Observes ShutdownEvent ev) {
        LOGGER.info("onStop");
        stopKafkaConnection();
    }

    private void stopKafkaConnection() {
        vertx.undeploy(deploymentId, handler->{
            if(handler.succeeded()){
                vertx.close();
            }
        });
    }


}
