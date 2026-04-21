# Request Dispatch Benchmarks for gRPCServer.jl
#
# These benchmarks measure the performance of the request dispatch path:
# - Method lookup by path
# - Handler invocation overhead
# - Context creation

using BenchmarkTools
using gRPCServer
using gRPCServer: ServiceRegistry, RequestDispatcher, MethodDescriptor, ServiceDescriptor,
                  MethodType, ServerContext, PeerInfo, lookup_method, register!,
                  create_context_from_headers
using Sockets: IPv4

# Sample handler for benchmarks (minimal work)
function benchmark_handler(ctx::ServerContext, request)
    return request
end

"""
    create_dispatch_benchmarks() -> BenchmarkGroup

Create benchmarks for request dispatch operations.
"""
function create_dispatch_benchmarks()
    suite = BenchmarkGroup()

    # Setup: Create a service registry with some methods
    registry = ServiceRegistry()

    # Create sample methods
    methods = Dict{String, MethodDescriptor}(
        "Method1" => MethodDescriptor("Method1", MethodType.UNARY, "test.Request", "test.Response", benchmark_handler),
        "Method2" => MethodDescriptor("Method2", MethodType.UNARY, "test.Request", "test.Response", benchmark_handler),
        "Method3" => MethodDescriptor("Method3", MethodType.SERVER_STREAMING, "test.Request", "test.Response", benchmark_handler),
    )

    # Create sample service
    service = ServiceDescriptor("test.BenchmarkService", methods)
    register!(registry, service)

    # Pre-compute paths for lookup
    path1 = "/test.BenchmarkService/Method1"
    path2 = "/test.BenchmarkService/Method2"
    invalid_path = "/test.Unknown/Method"

    # Benchmark: Method lookup (successful)
    suite["method_lookup"] = @benchmarkable begin
        lookup_method($registry, $path1)
    end

    # Benchmark: Method lookup (not found)
    suite["method_lookup_miss"] = @benchmarkable begin
        lookup_method($registry, $invalid_path)
    end

    # Benchmark: Context creation from headers
    headers = [
        (":method", "POST"),
        (":scheme", "http"),
        (":path", "/test.BenchmarkService/Method1"),
        (":authority", "localhost:50051"),
        ("content-type", "application/grpc"),
        ("grpc-timeout", "30S"),
        ("x-custom-header", "value"),
    ]
    peer = PeerInfo(IPv4("127.0.0.1"), 12345)

    suite["context_creation"] = @benchmarkable begin
        create_context_from_headers($headers, $peer)
    end

    # Benchmark: Simple context creation (no headers parsing)
    suite["context_simple"] = @benchmarkable begin
        ServerContext(
            method="/test.BenchmarkService/Method1",
            authority="localhost:50051"
        )
    end

    # Benchmark: PeerInfo creation
    suite["peer_info_creation"] = @benchmarkable begin
        PeerInfo(IPv4("127.0.0.1"), 12345)
    end

    # Benchmark: Unbounded request admission (default production path)
    server_unbounded = GRPCServer("127.0.0.1", 50051)
    server_unbounded.status = ServerStatus.RUNNING

    suite["request_admission_unbounded"] = @benchmarkable begin
        status = gRPCServer._acquire_request_slot!($server_unbounded)
        status == :acquired || error("unexpected admission status: $status")
        gRPCServer._release_request_slot!($server_unbounded)
    end

    # Benchmark: Bounded request admission without contention
    server_bounded = GRPCServer(
        "127.0.0.1",
        50052;
        max_concurrent_requests=8,
        max_queued_requests=32,
    )
    server_bounded.status = ServerStatus.RUNNING

    suite["request_admission_bounded"] = @benchmarkable begin
        status = gRPCServer._acquire_request_slot!($server_bounded)
        status == :acquired || error("unexpected admission status: $status")
        gRPCServer._release_request_slot!($server_bounded)
    end

    # Benchmark: Admission state snapshot for drain/monitoring checks
    suite["request_admission_state"] = @benchmarkable begin
        gRPCServer._request_admission_state($server_bounded)
    end

    return suite
end
