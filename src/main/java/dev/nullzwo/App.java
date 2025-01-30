package dev.nullzwo;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;

public class App {

    public static Meter METER;

    public static void main(String[] args) {
        var server = PrometheusHttpServer.create();
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));

        var meterProvider = SdkMeterProvider.builder().registerMetricReader(server).build();
        METER = meterProvider.get("faire-pipes");

        var simulation = new Simulation();
        Runtime.getRuntime().addShutdownHook(new Thread(simulation::stop));

        simulation.run();

        System.out.println("Everything running.");
    }
}
