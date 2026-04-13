# Unit tests for GRPCServer lifecycle

using Test
using Sockets
using gRPCServer

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

    @testset "Live bidi frame pump dispatches sibling streams without consuming active stream" begin
        server = GRPCServer("0.0.0.0", 50051; enable_health_check=true)
        gRPCServer.register_builtin_services!(server)

        conn = gRPCServer.HTTP2Connection()
        conn.state = gRPCServer.ConnectionState.OPEN
        peer = gRPCServer.PeerInfo(ip"127.0.0.1", 50051)

        function seed_health_check_stream(
            conn::gRPCServer.HTTP2Connection,
            stream_id::UInt32;
            buffered_request::Union{Nothing, Vector{UInt8}}=nothing,
            end_stream_on_headers::Bool=false,
        )
            stream = gRPCServer.create_stream(conn, stream_id)
            stream.request_headers = [
                (":path", "/grpc.health.v1.Health/Check"),
                ("content-type", "application/grpc"),
                ("te", "trailers"),
            ]
            stream.headers_complete = true
            gRPCServer.receive_headers!(stream, end_stream_on_headers)
            if buffered_request !== nothing
                write(stream.data_buffer, buffered_request)
            end
            return stream
        end

        request_bytes = gRPCServer.encode_grpc_message(
            gRPCServer.serialize_message(gRPCServer.HealthCheckRequest(""));
            compressed=false,
        )

        active_stream =
            seed_health_check_stream(conn, UInt32(1); buffered_request=request_bytes)
        sibling_stream = seed_health_check_stream(conn, UInt32(3))

        io = IOBuffer()
        sibling_frame = gRPCServer.data_frame(3, request_bytes; end_stream=true)
        gRPCServer.process_incoming_frame!(
            server,
            conn,
            io,
            peer,
            sibling_frame;
            exclude_stream_id=UInt32(1),
        )

        @test gRPCServer.get_stream(conn, UInt32(1)) === active_stream
        @test gRPCServer.has_complete_grpc_message(active_stream)
        @test gRPCServer.get_stream(conn, UInt32(3)) === nothing
        @test !isempty(take!(io))
    end

    @testset "Unary streams are not redispatched on trailing empty END_STREAM" begin
        server = GRPCServer("0.0.0.0", 50051)
        calls = Ref(0)

        descriptor = ServiceDescriptor(
            "test.DispatchOnce",
            Dict(
                "Check" => MethodDescriptor(
                    "Check",
                    MethodType.UNARY,
                    gRPCServer.HealthCheckRequest,
                    gRPCServer.HealthCheckResponse,
                    (ctx, req) -> begin
                        calls[] += 1
                        return gRPCServer.HealthCheckResponse(
                            gRPCServer.var"HealthCheckResponse.ServingStatus".SERVING,
                        )
                    end,
                ),
            ),
            nothing,
        )
        gRPCServer.register_service!(server.dispatcher, descriptor)
        server.health_status[descriptor.name] = HealthStatus.SERVING

        conn = gRPCServer.HTTP2Connection()
        conn.state = gRPCServer.ConnectionState.OPEN
        peer = gRPCServer.PeerInfo(ip"127.0.0.1", 50051)

        stream = gRPCServer.create_stream(conn, UInt32(1))
        stream.request_headers = [
            (":path", "/test.DispatchOnce/Check"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
        ]
        stream.headers_complete = true
        gRPCServer.receive_headers!(stream, false)

        request_bytes = gRPCServer.encode_grpc_message(
            gRPCServer.serialize_message(gRPCServer.HealthCheckRequest(""));
            compressed=false,
        )

        io = IOBuffer()
        gRPCServer.process_incoming_frame!(
            server,
            conn,
            io,
            peer,
            gRPCServer.data_frame(1, request_bytes; end_stream=false),
        )

        @test calls[] == 1
        @test gRPCServer.get_stream(conn, UInt32(1)) === stream
        @test stream.request_dispatched

        gRPCServer.process_incoming_frame!(
            server,
            conn,
            io,
            peer,
            gRPCServer.data_frame(1, UInt8[]; end_stream=true),
        )

        @test calls[] == 1
        @test gRPCServer.get_stream(conn, UInt32(1)) === nothing
    end
end
