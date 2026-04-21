using Logging
using Printf
using Statistics
using Sockets
using gRPCServer
using PureHTTP2
import ProtoBuf as PB

include(joinpath(@__DIR__, "..", "test", "TestUtils.jl"))
using .TestUtils

const DEFAULT_REQUEST_PATH = "/benchmark.LiveUnarySoakService/Echo"

struct BenchmarkPayload
    payload::Vector{UInt8}
end

PB.default_values(::Type{BenchmarkPayload}) = (;payload = UInt8[])
PB.field_numbers(::Type{BenchmarkPayload}) = (;payload = 1)

function PB.decode(d::PB.AbstractProtoDecoder, ::Type{<:BenchmarkPayload})
    payload = UInt8[]
    while !PB.message_done(d)
        field_number, wire_type = PB.decode_tag(d)
        if field_number == 1
            payload = PB.decode(d, Vector{UInt8})
        else
            Base.skip(d, wire_type)
        end
    end
    return BenchmarkPayload(payload)
end

function PB.encode(e::PB.AbstractProtoEncoder, x::BenchmarkPayload)
    initpos = position(e.io)
    !isempty(x.payload) && PB.encode(e, 1, x.payload)
    return position(e.io) - initpos
end

function PB._encoded_size(x::BenchmarkPayload)
    encoded_size = 0
    !isempty(x.payload) && (encoded_size += PB._encoded_size(x.payload, 1))
    return encoded_size
end

function encode_proto_message(message)::Vector{UInt8}
    io = IOBuffer()
    encoder = PB.ProtoEncoder(io)
    PB.encode(encoder, message)
    return take!(io)
end

mutable struct LiveHTTP2UnarySession
    port::Int
    tcp::Sockets.TCPSocket
    conn::PureHTTP2.HTTP2Connection
    pending::Dict{UInt32, PureHTTP2.ClientStreamState}
end

function open_live_http2_unary_session(port::Int)
    tcp = Sockets.connect(Sockets.IPv4("127.0.0.1"), port)
    conn = PureHTTP2.HTTP2Connection()
    conn.state = PureHTTP2.ConnectionState.OPEN
    conn.next_stream_id = UInt32(1)
    conn.pending_settings_ack = true
    PureHTTP2._write_preface_and_settings!(conn, tcp)
    return LiveHTTP2UnarySession(
        port,
        tcp,
        conn,
        Dict{UInt32, PureHTTP2.ClientStreamState}(),
    )
end

function close_live_http2_unary_session!(session::LiveHTTP2UnarySession)
    close(session.tcp)
    return nothing
end

function await_server_ping!(session::LiveHTTP2UnarySession; timeout::Float64=1.0)
    deadline = time() + timeout
    while time() < deadline
        frame_timeout = max(deadline - time(), 0.05)
        frame = try
            gRPCServer.read_frame(session.tcp; idle_timeout=frame_timeout)
        catch err
            if err isa gRPCServer.IdleConnectionTimeoutError
                continue
            end
            rethrow()
        end
        frame === nothing && error("Transport EOF before keepalive ping arrived")
        if frame.header.frame_type == PureHTTP2.FrameType.PING &&
           !PureHTTP2.has_flag(frame.header, PureHTTP2.FrameFlags.ACK)
            return frame
        end
    end
    error("Timed out waiting for server keepalive ping")
end

function ack_server_ping!(session::LiveHTTP2UnarySession, ping::PureHTTP2.Frame)
    gRPCServer.write_frame(session.tcp, PureHTTP2.ping_frame(ping.payload; ack=true))
    return nothing
end

function ack_keepalive_rounds!(
    session::LiveHTTP2UnarySession;
    rounds::Int,
    timeout::Float64=1.0,
)
    for _ in 1:rounds
        ping = await_server_ping!(session; timeout=timeout)
        ack_server_ping!(session, ping)
    end
    return rounds
end

function send_live_unary_request!(
    session::LiveHTTP2UnarySession,
    path::String,
    proto_payload::Vector{UInt8},
)
    stream_id = PureHTTP2._write_request!(
        session.conn,
        session.tcp,
        [
            (":method", "POST"),
            (":path", path),
            (":scheme", "http"),
            (":authority", "127.0.0.1:$(session.port)"),
            ("content-type", "application/grpc"),
            ("te", "trailers"),
        ],
        build_grpc_message(proto_payload),
    )
    session.pending[stream_id] = PureHTTP2.ClientStreamState(stream_id)
    return stream_id
end

function await_live_unary_response!(
    session::LiveHTTP2UnarySession,
    stream_id::UInt32;
    max_frame_size::Int=PureHTTP2.DEFAULT_MAX_FRAME_SIZE,
)
    while !PureHTTP2.is_closed(session.conn)
        state = session.pending[stream_id]
        if state.headers_complete && state.end_stream_received
            break
        end

        frame = PureHTTP2._read_one_frame(session.tcp, max_frame_size)
        if frame === nothing
            if state.headers_complete && state.end_stream_received
                break
            end
            error("Transport EOF before response complete for stream $stream_id")
        end

        exit_loop = PureHTTP2.client_dispatch_frame!(
            session.conn,
            session.tcp,
            session.pending,
            frame,
        )
        if exit_loop
            updated_state = session.pending[stream_id]
            if !(updated_state.headers_complete && updated_state.end_stream_received)
                error("GOAWAY before response complete for stream $stream_id")
            end
        end
    end

    final_state = pop!(session.pending, stream_id)
    return take!(final_state.response_body)
end

function parse_args(args)
    config = Dict{String, Any}(
        "concurrency" => 16,
        "requests_per_session" => 32,
        "warmup_requests" => 4,
        "payload_bytes" => 128,
        "post_request_keepalive_rounds" => 1,
        "keepalive_interval" => 0.2,
        "keepalive_timeout" => 0.5,
        "max_concurrent_requests" => 16,
        "max_queued_requests" => 64,
        "graceful_timeout" => 5.0,
        "handler_sleep_ms" => 0.0,
    )

    parsers = Dict(
        "concurrency" => x -> parse(Int, x),
        "requests_per_session" => x -> parse(Int, x),
        "warmup_requests" => x -> parse(Int, x),
        "payload_bytes" => x -> parse(Int, x),
        "post_request_keepalive_rounds" => x -> parse(Int, x),
        "keepalive_interval" => x -> parse(Float64, x),
        "keepalive_timeout" => x -> parse(Float64, x),
        "max_concurrent_requests" => x -> parse(Int, x),
        "max_queued_requests" => x -> parse(Int, x),
        "graceful_timeout" => x -> parse(Float64, x),
        "handler_sleep_ms" => x -> parse(Float64, x),
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("-h", "--help")
            println("Usage: julia --project=benchmark benchmark/live_unary_soak.jl [options]")
            println("Options:")
            println("  --concurrency N")
            println("  --requests-per-session N")
            println("  --warmup-requests N")
            println("  --payload-bytes N")
            println("  --post-request-keepalive-rounds N")
            println("  --keepalive-interval SECONDS")
            println("  --keepalive-timeout SECONDS")
            println("  --max-concurrent-requests N")
            println("  --max-queued-requests N")
            println("  --graceful-timeout SECONDS")
            println("  --handler-sleep-ms MS")
            exit(0)
        elseif startswith(arg, "--")
            key = replace(arg[3:end], "-" => "_")
            haskey(config, key) || error("Unknown option: $arg")
            i < length(args) || error("Missing value for $arg")
            config[key] = parsers[key](args[i + 1])
            i += 2
        else
            error("Unknown positional argument: $arg")
        end
    end

    config["concurrency"] > 0 || error("concurrency must be positive")
    config["requests_per_session"] > 0 || error("requests_per_session must be positive")
    config["warmup_requests"] >= 0 || error("warmup_requests must be non-negative")
    config["payload_bytes"] >= 5 || error("payload_bytes must be at least 5")
    config["post_request_keepalive_rounds"] >= 0 ||
        error("post_request_keepalive_rounds must be non-negative")
    config["keepalive_interval"] > 0 || error("keepalive_interval must be positive")
    config["keepalive_timeout"] > 0 || error("keepalive_timeout must be positive")
    config["max_concurrent_requests"] > 0 || error("max_concurrent_requests must be positive")
    config["max_queued_requests"] >= 0 || error("max_queued_requests must be non-negative")
    config["graceful_timeout"] > 0 || error("graceful_timeout must be positive")
    config["handler_sleep_ms"] >= 0 || error("handler_sleep_ms must be non-negative")

    return config
end

function make_payload(session_idx::Int, request_idx::Int, payload_bytes::Int)
    return [
        UInt8((session_idx + request_idx + offset) % 0xff) for offset in 0:(payload_bytes - 1)
    ]
end

function make_payloads(session_idx::Int, request_count::Int, payload_bytes::Int)
    return [make_payload(session_idx, request_idx, payload_bytes) for request_idx in 1:request_count]
end

function timed_assert(predicate::Function, timeout::Float64, message::String)
    timedwait(predicate, timeout; pollint=min(timeout / 20, 0.05)) === :ok || error(message)
    return nothing
end

function spawn_worker(f::Function)
    if Threads.nthreads() > 1
        return Threads.@spawn f()
    end
    return @async f()
end

function run_session_workload!(
    session::LiveHTTP2UnarySession,
    path::String,
    payloads::Vector{Vector{UInt8}};
    post_request_keepalive_rounds::Int,
    keepalive_timeout::Float64,
)
    latencies_ns = Vector{Int64}(undef, length(payloads))
    for (idx, payload) in pairs(payloads)
        proto_payload = encode_proto_message(BenchmarkPayload(payload))
        started = time_ns()
        stream_id = send_live_unary_request!(session, path, proto_payload)
        response_body = await_live_unary_response!(session, stream_id)
        latencies_ns[idx] = time_ns() - started
        response_body == build_grpc_message(proto_payload) ||
            error("Unexpected response body for request $idx")
    end

    if post_request_keepalive_rounds > 0
        ack_keepalive_rounds!(
            session;
            rounds=post_request_keepalive_rounds,
            timeout=max(keepalive_timeout * 4, 1.0),
        )
    end

    return latencies_ns
end

function print_summary(config, total_requests, elapsed_ns, latencies_ns, graceful_stop_ns, server)
    elapsed_seconds = elapsed_ns / 1.0e9
    throughput = total_requests / elapsed_seconds
    latencies_ms = latencies_ns ./ 1.0e6
    state = gRPCServer._request_admission_state(server)

    println("Live unary soak completed")
    println("  Julia threads:             $(Threads.nthreads())")
    println("  Concurrency:               $(config["concurrency"])")
    println("  Requests per session:      $(config["requests_per_session"])")
    println("  Payload bytes:             $(config["payload_bytes"])")
    println("  Max concurrent requests:   $(config["max_concurrent_requests"])")
    println("  Max queued requests:       $(config["max_queued_requests"])")
    println("  Keepalive interval:        $(config["keepalive_interval"]) s")
    println("  Keepalive timeout:         $(config["keepalive_timeout"]) s")
    println("  Total requests:            $total_requests")
    @printf("  Elapsed:                   %.3f s\n", elapsed_seconds)
    @printf("  Throughput:                %.2f req/s\n", throughput)
    @printf("  Latency p50:               %.3f ms\n", median(latencies_ms))
    @printf("  Latency p95:               %.3f ms\n", quantile(latencies_ms, 0.95))
    @printf("  Latency max:               %.3f ms\n", maximum(latencies_ms))
    @printf("  Graceful stop:             %.3f ms\n", graceful_stop_ns / 1.0e6)
    println("  Remaining connections:     $(length(server.connections))")
    println("  Remaining connection tasks: $(length(server.connection_tasks))")
    println("  Admission active/queued:   $(state.active_requests)/$(state.queued_requests)")
end

function main(args)
    config = parse_args(args)

    descriptor = ServiceDescriptor(
        "benchmark.LiveUnarySoakService",
        Dict(
                "Echo" => MethodDescriptor(
                    "Echo",
                    MethodType.UNARY,
                    BenchmarkPayload,
                    BenchmarkPayload,
                    (ctx, req::BenchmarkPayload) -> begin
                        if config["handler_sleep_ms"] > 0
                            sleep(config["handler_sleep_ms"] / 1000)
                        end
                    return req
                end,
            ),
        ),
        nothing,
    )

    with_logger(ConsoleLogger(stderr, Logging.Error)) do
        with_test_server(
            keepalive_interval=config["keepalive_interval"],
            keepalive_timeout=config["keepalive_timeout"],
            max_concurrent_requests=config["max_concurrent_requests"],
            max_queued_requests=config["max_queued_requests"],
        ) do ts
            gRPCServer.register_service!(ts.server.dispatcher, descriptor)
            ts.server.health_status["benchmark.LiveUnarySoakService"] = HealthStatus.SERVING

            sessions = [
                open_live_http2_unary_session(ts.port) for _ in 1:config["concurrency"]
            ]
            stopper = nothing
            try
                timed_assert(
                    () -> length(ts.server.connections) == config["concurrency"],
                    2.0,
                    "Timed out waiting for all live HTTP/2 sessions to connect",
                )

                for (session_idx, session) in pairs(sessions)
                    warmup_payloads = make_payloads(
                        session_idx,
                        config["warmup_requests"],
                        config["payload_bytes"],
                    )
                    run_session_workload!(
                        session,
                        DEFAULT_REQUEST_PATH,
                        warmup_payloads;
                        post_request_keepalive_rounds=0,
                        keepalive_timeout=config["keepalive_timeout"],
                    )
                end

                worker_payloads = [
                    make_payloads(
                        session_idx,
                        config["requests_per_session"],
                        config["payload_bytes"],
                    ) for session_idx in 1:config["concurrency"]
                ]

                started = time_ns()
                tasks = [
                    spawn_worker(() -> run_session_workload!(
                        sessions[i],
                        DEFAULT_REQUEST_PATH,
                        worker_payloads[i];
                        post_request_keepalive_rounds=config["post_request_keepalive_rounds"],
                        keepalive_timeout=config["keepalive_timeout"],
                    )) for i in 1:length(sessions)
                ]
                latency_chunks = fetch.(tasks)
                elapsed_ns = time_ns() - started

                flat_latencies = reduce(vcat, latency_chunks)
                total_requests = length(flat_latencies)
                expected_requests = config["concurrency"] * config["requests_per_session"]
                total_requests == expected_requests ||
                    error("Expected $expected_requests requests, observed $total_requests")

                stop_started = time_ns()
                stopper = @async stop!(ts.server; force=false, timeout=config["graceful_timeout"])
                timed_assert(
                    () -> istaskdone(stopper),
                    config["graceful_timeout"] + 1.0,
                    "Graceful stop did not finish within the expected timeout",
                )
                wait(stopper)
                graceful_stop_ns = time_ns() - stop_started

                timed_assert(
                    () -> isempty(ts.server.connections),
                    1.0,
                    "Connections remained registered after graceful stop",
                )
                timed_assert(
                    () -> isempty(ts.server.connection_tasks),
                    1.0,
                    "Connection tasks remained after graceful stop",
                )
                state = gRPCServer._request_admission_state(ts.server)
                state.active_requests == 0 || error("Active request count did not drain to zero")
                state.queued_requests == 0 || error("Queued request count did not drain to zero")

                print_summary(
                    config,
                    total_requests,
                    elapsed_ns,
                    flat_latencies,
                    graceful_stop_ns,
                    ts.server,
                )
            finally
                for session in sessions
                    try
                        close_live_http2_unary_session!(session)
                    catch
                    end
                end
                if stopper !== nothing && !istaskdone(stopper)
                    try
                        wait(stopper)
                    catch
                    end
                end
            end
        end
    end
end

main(ARGS)
