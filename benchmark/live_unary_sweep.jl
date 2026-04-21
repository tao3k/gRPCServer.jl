using Logging
using Printf
using Statistics

include(joinpath(@__DIR__, "live_unary_soak.jl"))

function default_sweep_config()
    return Dict{String, Any}(
        "concurrency_list" => [16, 32, 64],
        "rounds" => 3,
        "requests_per_session" => 32,
        "warmup_requests" => 4,
        "payload_bytes" => 128,
        "post_request_keepalive_rounds" => 1,
        "keepalive_interval" => 0.2,
        "keepalive_timeout" => 0.5,
        "max_concurrent_requests" => nothing,
        "max_queued_requests" => nothing,
        "graceful_timeout" => 5.0,
        "handler_sleep_ms" => 0.0,
    )
end

function parse_concurrency_list(value::String)
    values = [parse(Int, strip(part)) for part in split(value, ",") if !isempty(strip(part))]
    isempty(values) && error("concurrency_list must not be empty")
    return values
end

function parse_optional_int(value::String)
    lowercase(strip(value)) == "auto" && return nothing
    return parse(Int, value)
end

function validate_sweep_config!(config)
    !isempty(config["concurrency_list"]) || error("concurrency_list must not be empty")
    all(config["concurrency_list"] .> 0) || error("concurrency_list values must be positive")
    config["rounds"] > 0 || error("rounds must be positive")
    config["requests_per_session"] > 0 || error("requests_per_session must be positive")
    config["warmup_requests"] >= 0 || error("warmup_requests must be non-negative")
    config["payload_bytes"] >= 5 || error("payload_bytes must be at least 5")
    config["post_request_keepalive_rounds"] >= 0 ||
        error("post_request_keepalive_rounds must be non-negative")
    config["keepalive_interval"] > 0 || error("keepalive_interval must be positive")
    config["keepalive_timeout"] > 0 || error("keepalive_timeout must be positive")
    isnothing(config["max_concurrent_requests"]) || config["max_concurrent_requests"] > 0 ||
        error("max_concurrent_requests must be positive when set")
    isnothing(config["max_queued_requests"]) || config["max_queued_requests"] >= 0 ||
        error("max_queued_requests must be non-negative when set")
    config["graceful_timeout"] > 0 || error("graceful_timeout must be positive")
    config["handler_sleep_ms"] >= 0 || error("handler_sleep_ms must be non-negative")
    return config
end

function parse_args(args)
    config = default_sweep_config()
    parsers = Dict(
        "concurrency_list" => parse_concurrency_list,
        "rounds" => x -> parse(Int, x),
        "requests_per_session" => x -> parse(Int, x),
        "warmup_requests" => x -> parse(Int, x),
        "payload_bytes" => x -> parse(Int, x),
        "post_request_keepalive_rounds" => x -> parse(Int, x),
        "keepalive_interval" => x -> parse(Float64, x),
        "keepalive_timeout" => x -> parse(Float64, x),
        "max_concurrent_requests" => parse_optional_int,
        "max_queued_requests" => parse_optional_int,
        "graceful_timeout" => x -> parse(Float64, x),
        "handler_sleep_ms" => x -> parse(Float64, x),
    )

    i = 1
    while i <= length(args)
        arg = args[i]
        if arg in ("-h", "--help")
            println("Usage: julia --project=benchmark benchmark/live_unary_sweep.jl [options]")
            println("Options:")
            println("  --concurrency-list 16,32,64")
            println("  --rounds N")
            println("  --requests-per-session N")
            println("  --warmup-requests N")
            println("  --payload-bytes N")
            println("  --post-request-keepalive-rounds N")
            println("  --keepalive-interval SECONDS")
            println("  --keepalive-timeout SECONDS")
            println("  --max-concurrent-requests N|auto")
            println("  --max-queued-requests N|auto")
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

    return validate_sweep_config!(config)
end

function resolve_run_config(sweep_config, concurrency::Int)
    config = default_config()
    config["concurrency"] = concurrency
    config["requests_per_session"] = sweep_config["requests_per_session"]
    config["warmup_requests"] = sweep_config["warmup_requests"]
    config["payload_bytes"] = sweep_config["payload_bytes"]
    config["post_request_keepalive_rounds"] = sweep_config["post_request_keepalive_rounds"]
    config["keepalive_interval"] = sweep_config["keepalive_interval"]
    config["keepalive_timeout"] = sweep_config["keepalive_timeout"]
    config["max_concurrent_requests"] = something(
        sweep_config["max_concurrent_requests"],
        concurrency,
    )
    config["max_queued_requests"] = something(
        sweep_config["max_queued_requests"],
        4 * concurrency,
    )
    config["graceful_timeout"] = sweep_config["graceful_timeout"]
    config["handler_sleep_ms"] = sweep_config["handler_sleep_ms"]
    return validate_config!(config)
end

function aggregate_results(results)
    throughputs = getfield.(results, :throughput_rps)
    p50s = getfield.(results, :latency_p50_ms)
    p95s = getfield.(results, :latency_p95_ms)
    maxes = getfield.(results, :latency_max_ms)
    stops = getfield.(results, :graceful_stop_ms)
    first_result = first(results)

    return (
        threads=first_result.threads,
        concurrency=first_result.concurrency,
        rounds=length(results),
        total_requests_per_round=first_result.total_requests,
        requests_per_session=first_result.requests_per_session,
        throughput_avg_rps=mean(throughputs),
        throughput_min_rps=minimum(throughputs),
        throughput_max_rps=maximum(throughputs),
        latency_p50_avg_ms=mean(p50s),
        latency_p95_avg_ms=mean(p95s),
        latency_p95_max_ms=maximum(p95s),
        latency_max_ms=maximum(maxes),
        graceful_stop_avg_ms=mean(stops),
        graceful_stop_max_ms=maximum(stops),
    )
end

function print_round_result(round_idx::Int, round_total::Int, metrics; io::IO=stdout)
    @printf(
        io,
        "Round %d/%d concurrency=%d throughput=%.2f req/s p95=%.3f ms stop=%.3f ms\n",
        round_idx,
        round_total,
        metrics.concurrency,
        metrics.throughput_rps,
        metrics.latency_p95_ms,
        metrics.graceful_stop_ms,
    )
end

function print_sweep_summary(aggregates; io::IO=stdout)
    println(io, "")
    println(io, "Live unary sweep summary")
    println(io, "")
    println(
        io,
        "| concurrency | rounds | requests/round | throughput avg (req/s) | throughput min | throughput max | p50 avg (ms) | p95 avg (ms) | p95 max (ms) | max latency (ms) | stop avg (ms) | stop max (ms) |",
    )
    println(
        io,
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    )
    for aggregate in aggregates
        @printf(
            io,
            "| %d | %d | %d | %.2f | %.2f | %.2f | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f |\n",
            aggregate.concurrency,
            aggregate.rounds,
            aggregate.total_requests_per_round,
            aggregate.throughput_avg_rps,
            aggregate.throughput_min_rps,
            aggregate.throughput_max_rps,
            aggregate.latency_p50_avg_ms,
            aggregate.latency_p95_avg_ms,
            aggregate.latency_p95_max_ms,
            aggregate.latency_max_ms,
            aggregate.graceful_stop_avg_ms,
            aggregate.graceful_stop_max_ms,
        )
    end
end

function run_sweep(sweep_config; logger=ConsoleLogger(stderr, Logging.Error))
    results_by_concurrency = Dict{Int, Vector{NamedTuple}}()

    for concurrency in sweep_config["concurrency_list"]
        run_config = resolve_run_config(sweep_config, concurrency)
        metrics = NamedTuple[]
        for round_idx in 1:sweep_config["rounds"]
            round_metrics = run_live_unary_soak(run_config; logger=logger)
            push!(metrics, round_metrics)
            print_round_result(round_idx, sweep_config["rounds"], round_metrics)
        end
        results_by_concurrency[concurrency] = metrics
    end

    ordered = [aggregate_results(results_by_concurrency[c]) for c in sweep_config["concurrency_list"]]
    return ordered
end

function main(args)
    sweep_config = parse_args(args)
    aggregates = run_sweep(sweep_config)
    print_sweep_summary(aggregates)
end

if abspath(PROGRAM_FILE) == @__FILE__
    main(ARGS)
end
