# Unit tests for GRPCServer lifecycle

using Test
using gRPCServer
using PureHTTP2
using Sockets

function test_available_port()
    server = listen(IPv4(0), 0)
    _, port = getsockname(server)
    close(server)
    return Int(port)
end

@testset "GRPCServer Unit Tests" begin
    @testset "Server Creation" begin
        # Basic creation
        server = GRPCServer("0.0.0.0", 50051)
        @test server.host == "0.0.0.0"
        @test server.port == 50051
        @test server.status == ServerStatus.STOPPED
        @test isempty(services(server))

        # Creation with custom port
        server2 = GRPCServer("localhost", 8080)
        @test server2.host == "localhost"
        @test server2.port == 8080
    end

    @testset "Server Configuration" begin
        server = GRPCServer(
            "0.0.0.0", 50051;
            max_message_size = 8 * 1024 * 1024,
            max_concurrent_streams = 200,
            enable_health_check = true,
            enable_reflection = true,
            debug_mode = true
        )

        @test server.config.max_message_size == 8 * 1024 * 1024
        @test server.config.max_concurrent_streams == 200
        @test server.config.enable_health_check == true
        @test server.config.enable_reflection == true
        @test server.config.debug_mode == true
    end

    @testset "HTTP/2 Backend Configuration" begin
        backend = PureHTTP2Backend()
        conn = create_connection(backend)
        stream = PureHTTP2.create_stream(conn, UInt32(1))

        @test conn.local_settings.initial_window_size ==
              gRPCServer.DEFAULT_PUREHTTP2_INITIAL_WINDOW_SIZE
        @test PureHTTP2.available(conn.flow_controller.connection_window) ==
              gRPCServer.DEFAULT_PUREHTTP2_INITIAL_WINDOW_SIZE
        @test PureHTTP2.available(
            PureHTTP2.get_stream_window(conn.flow_controller, stream.id),
        ) == gRPCServer.DEFAULT_PUREHTTP2_INITIAL_WINDOW_SIZE

        tuned_backend = PureHTTP2Backend(initial_window_size=262_144)
        tuned_conn = create_connection(tuned_backend)
        PureHTTP2.create_stream(tuned_conn, UInt32(3))
        @test tuned_conn.local_settings.initial_window_size == 262_144
        @test PureHTTP2.available(tuned_conn.flow_controller.connection_window) == 262_144
        @test PureHTTP2.available(
            PureHTTP2.get_stream_window(tuned_conn.flow_controller, UInt32(3)),
        ) == 262_144
    end

    @testset "Inbound WINDOW_UPDATE threshold stays large-transport safe" begin
        backend = PureHTTP2Backend()
        conn = create_connection(backend)
        stream = PureHTTP2.create_stream(conn, UInt32(5))
        stream_window = PureHTTP2.get_stream_window(conn.flow_controller, stream.id)
        @test !isnothing(stream_window)

        threshold_ratio = gRPCServer._inbound_window_update_threshold_ratio(conn)
        threshold_bytes = Int(round(threshold_ratio * conn.flow_controller.initial_stream_window))
        @test threshold_bytes == gRPCServer.MAX_INBOUND_WINDOW_UPDATE_BYTES

        for chunk_bytes in (17, 264, 16_384, 16_384, 16_384, 15_578)
            @test PureHTTP2.consume!(stream_window, chunk_bytes)
            @test PureHTTP2.consume!(conn.flow_controller.connection_window, chunk_bytes)
        end

        updates = PureHTTP2.generate_window_updates(
            conn.flow_controller;
            threshold_ratio=threshold_ratio,
        )
        @test any(
            frame ->
                frame.header.frame_type == PureHTTP2.FrameType.WINDOW_UPDATE &&
                frame.header.stream_id == 0x00000000,
            updates,
        )
        @test any(
            frame ->
                frame.header.frame_type == PureHTTP2.FrameType.WINDOW_UPDATE &&
                frame.header.stream_id == stream.id,
            updates,
        )
    end

    @testset "Server Status" begin
        server = GRPCServer("0.0.0.0", 50051)

        # Initial status
        @test server.status == ServerStatus.STOPPED

        # Status enum values
        @test ServerStatus.STOPPED isa ServerStatus.T
        @test ServerStatus.STARTING isa ServerStatus.T
        @test ServerStatus.RUNNING isa ServerStatus.T
        @test ServerStatus.DRAINING isa ServerStatus.T
        @test ServerStatus.STOPPING isa ServerStatus.T
    end

    @testset "Server Show Method" begin
        server = GRPCServer("0.0.0.0", 50051)
        str = sprint(show, server)
        @test occursin("GRPCServer", str)
        @test occursin("0.0.0.0:50051", str)
        @test occursin("STOPPED", str)
    end

    @testset "Service Registration" begin
        server = GRPCServer("0.0.0.0", 50051)

        # Create a mock service descriptor
        descriptor = ServiceDescriptor(
            "test.TestService",
            Dict(
                "TestMethod" => MethodDescriptor(
                    "TestMethod",
                    MethodType.UNARY,
                    "test.TestRequest",
                    "test.TestResponse",
                    (ctx, req) -> req
                )
            ),
            nothing
        )

        # Register service directly via dispatcher (register! expects service_descriptor interface)
        gRPCServer.register_service!(server.dispatcher, descriptor)
        server.health_status[descriptor.name] = HealthStatus.SERVING
        @test "test.TestService" in services(server)

        # Cannot register same service twice
        @test_throws ServiceAlreadyRegisteredError gRPCServer.register_service!(server.dispatcher, descriptor)
    end

    @testset "Health Status" begin
        server = GRPCServer("0.0.0.0", 50051)

        # Set overall health
        set_health!(server, HealthStatus.SERVING)
        @test get_health(server) == HealthStatus.SERVING

        # Set service-specific health
        set_health!(server, "my.Service", HealthStatus.NOT_SERVING)
        @test get_health(server, "my.Service") == HealthStatus.NOT_SERVING

        # Unknown service returns SERVICE_UNKNOWN
        @test get_health(server, "unknown.Service") == HealthStatus.SERVICE_UNKNOWN
    end

    @testset "Interceptor Registration" begin
        server = GRPCServer("0.0.0.0", 50051)

        # Add global interceptor
        add_interceptor!(server, LoggingInterceptor())

        # Add service-specific interceptor
        add_interceptor!(server, "test.Service", MetricsInterceptor())

        # Verify interceptors are registered (indirectly through dispatcher)
        @test length(server.dispatcher.interceptor_chain) == 1
    end

    @testset "Force stop drains background tasks" begin
        server = GRPCServer("127.0.0.1", test_available_port())
        client = nothing
        try
            start!(server)
            @test timedwait(() -> !isnothing(server.accept_task), 2.0) === :ok
            client = connect(IPv4("127.0.0.1"), server.port)
            @test timedwait(() -> length(server.connections) == 1, 2.0) === :ok
            @test timedwait(() -> !isempty(server.connection_tasks), 2.0) === :ok

            stop!(server; force=true, timeout=2.0)

            @test server.status == ServerStatus.STOPPED
            @test isnothing(server.accept_task)
            @test isempty(server.connections)
            @test isempty(server.connection_tasks)
            @test !Base.isopen(server)
        finally
            if client !== nothing
                try
                    close(client)
                catch
                end
            end
            if server.status != ServerStatus.STOPPED
                try
                    stop!(server; force=true, timeout=2.0)
                catch
                end
            end
        end
    end
end
