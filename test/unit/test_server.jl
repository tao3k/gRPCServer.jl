# Unit tests for GRPCServer lifecycle

using Test
using Dates
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

    @testset "Response send abort enforcement" begin
        backend = PureHTTP2Backend()
        conn = create_connection(backend)
        stream = PureHTTP2.create_stream(conn, UInt32(7))

        expired_ctx = ServerContext(deadline=now() - Millisecond(10))
        deadline_error = gRPCServer._response_send_abort_error(conn, stream.id; context=expired_ctx)
        @test deadline_error isa GRPCError
        @test deadline_error.code == StatusCode.DEADLINE_EXCEEDED

        cancelled_ctx = ServerContext()
        gRPCServer.cancel!(cancelled_ctx)
        cancelled_error = gRPCServer._response_send_abort_error(conn, stream.id; context=cancelled_ctx)
        @test cancelled_error isa StreamCancelledError

        stream.state = PureHTTP2.StreamState.CLOSED
        closed_error = gRPCServer._response_send_abort_error(conn, stream.id)
        @test closed_error isa StreamCancelledError

        open_stream = PureHTTP2.create_stream(conn, UInt32(9))
        open_stream.state = PureHTTP2.StreamState.OPEN
        active_ctx = ServerContext(deadline=now() + Second(1))
        @test isnothing(
            gRPCServer._response_send_abort_error(conn, open_stream.id; context=active_ctx),
        )
    end

    @testset "_send_data_with_flow_control! aborts before write when request is inactive" begin
        backend = PureHTTP2Backend()
        conn = create_connection(backend)
        stream = PureHTTP2.create_stream(conn, UInt32(11))
        io = IOBuffer()
        payload = UInt8[0x01, 0x02, 0x03]

        expired_ctx = ServerContext(deadline=now() - Millisecond(10))
        deadline_error = try
            gRPCServer._send_data_with_flow_control!(
                conn,
                io,
                stream.id,
                payload;
                context=expired_ctx,
            )
            nothing
        catch err
            err
        end
        @test deadline_error isa GRPCError
        @test deadline_error.code == StatusCode.DEADLINE_EXCEEDED

        cancelled_ctx = ServerContext()
        gRPCServer.cancel!(cancelled_ctx)
        cancelled_error = try
            gRPCServer._send_data_with_flow_control!(
                conn,
                io,
                stream.id,
                payload;
                context=cancelled_ctx,
            )
            nothing
        catch err
            err
        end
        @test cancelled_error isa StreamCancelledError
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

    @testset "Graceful drain does not wait for idle connections" begin
        server = GRPCServer("127.0.0.1", test_available_port(); drain_timeout=1.0)
        client = nothing
        runner = nothing
        stopper = nothing
        try
            runner = @async run(server)
            @test timedwait(() -> server.status == ServerStatus.RUNNING, 2.0) === :ok

            client = connect(IPv4("127.0.0.1"), server.port)
            @test timedwait(() -> length(server.connections) == 1, 2.0) === :ok

            stopper = @async stop!(server; force=false, timeout=1.0)
            @test timedwait(() -> server.status != ServerStatus.RUNNING, 2.0) === :ok
            @test timedwait(() -> istaskdone(stopper), 1.0) === :ok
            @test timedwait(() -> istaskdone(runner), 1.0) === :ok

            wait(stopper)
            wait(runner)

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
            if stopper !== nothing && !istaskdone(stopper)
                try
                    wait(stopper)
                catch
                end
            end
            if runner !== nothing && !istaskdone(runner)
                try
                    stop!(server; force=true, timeout=2.0)
                catch
                end
                try
                    wait(runner)
                catch
                end
            elseif server.status != ServerStatus.STOPPED
                try
                    stop!(server; force=true, timeout=2.0)
                catch
                end
            end
        end
    end
end
