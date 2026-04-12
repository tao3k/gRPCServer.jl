# AC3: Message Encoding Tests
# Tests per gRPC HTTP/2 Protocol Specification
# Reference: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md

using Test
using gRPCServer

mutable struct CountingIO <: IO
    inner::IOBuffer
    write_count::Int
    flush_count::Int
end

CountingIO() = CountingIO(IOBuffer(), 0, 0)

function Base.write(io::CountingIO, data::StridedVector{UInt8})
    io.write_count += 1
    return write(io.inner, data)
end

function Base.flush(io::CountingIO)
    io.flush_count += 1
    return nothing
end

# Include test utilities if not already loaded
if !isdefined(@__MODULE__, :TestUtils)
    include("../TestUtils.jl")
    using .TestUtils
end

# Include conformance test data
include("../fixtures/conformance_data.jl")
using .ConformanceData

@testset "AC3: Message Encoding" begin

    # =========================================================================
    # T021: Length-Prefixed Message Format
    # =========================================================================

    @testset "T021: Length-prefixed message format" begin

        @testset "Build message: 1 byte flag + 4 bytes length + data" begin
            data = UInt8[0x01, 0x02, 0x03, 0x04, 0x05]
            msg = TestUtils.build_grpc_message(data)

            # Total: 1 + 4 + 5 = 10 bytes
            @test length(msg) == 10

            # Compressed flag (byte 1)
            @test msg[1] == 0x00  # Not compressed

            # Length in big-endian (bytes 2-5)
            @test msg[2] == 0x00
            @test msg[3] == 0x00
            @test msg[4] == 0x00
            @test msg[5] == 0x05

            # Data (bytes 6-10)
            @test msg[6:10] == data
        end

        @testset "Build empty message" begin
            data = UInt8[]
            msg = TestUtils.build_grpc_message(data)

            @test length(msg) == 5  # Just header
            @test msg[1] == 0x00
            @test msg[2] == 0x00
            @test msg[3] == 0x00
            @test msg[4] == 0x00
            @test msg[5] == 0x00
        end

        @testset "Build large message (256 bytes)" begin
            data = zeros(UInt8, 256)
            msg = TestUtils.build_grpc_message(data)

            @test length(msg) == 5 + 256

            # Length = 256 = 0x00000100
            @test msg[2] == 0x00
            @test msg[3] == 0x00
            @test msg[4] == 0x01
            @test msg[5] == 0x00
        end

    end  # T021

    # =========================================================================
    # T022: Compressed Flag
    # =========================================================================

    @testset "T022: Compressed flag" begin

        @testset "Uncompressed message: flag = 0" begin
            data = UInt8[0xAA, 0xBB, 0xCC]
            msg = TestUtils.build_grpc_message(data; compressed=false)
            @test msg[1] == 0x00
        end

        @testset "Compressed message: flag = 1" begin
            data = UInt8[0xAA, 0xBB, 0xCC]
            msg = TestUtils.build_grpc_message(data; compressed=true)
            @test msg[1] == 0x01
        end

        @testset "Parse compressed flag" begin
            # Uncompressed
            msg_uncomp = UInt8[0x00, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03]
            compressed, _ = TestUtils.parse_grpc_message(msg_uncomp)
            @test !compressed

            # Compressed
            msg_comp = UInt8[0x01, 0x00, 0x00, 0x00, 0x03, 0x01, 0x02, 0x03]
            compressed, _ = TestUtils.parse_grpc_message(msg_comp)
            @test compressed
        end

    end  # T022

    # =========================================================================
    # T023: Message Length Encoding (Big-Endian)
    # =========================================================================

    @testset "T023: Message length big-endian encoding" begin

        @testset "Small message length" begin
            data = UInt8[0x01]  # 1 byte
            msg = TestUtils.build_grpc_message(data)
            # Length = 1 = 0x00000001
            @test msg[2:5] == UInt8[0x00, 0x00, 0x00, 0x01]
        end

        @testset "Medium message length (1024 bytes)" begin
            data = zeros(UInt8, 1024)
            msg = TestUtils.build_grpc_message(data)
            # Length = 1024 = 0x00000400
            @test msg[2:5] == UInt8[0x00, 0x00, 0x04, 0x00]
        end

        @testset "Large message length (65536 bytes)" begin
            data = zeros(UInt8, 65536)
            msg = TestUtils.build_grpc_message(data)
            # Length = 65536 = 0x00010000
            @test msg[2:5] == UInt8[0x00, 0x01, 0x00, 0x00]
        end

        @testset "Parse length correctly" begin
            # Message with length = 256
            msg = vcat(UInt8[0x00, 0x00, 0x00, 0x01, 0x00], zeros(UInt8, 256))
            _, data = TestUtils.parse_grpc_message(msg)
            @test length(data) == 256
        end

    end  # T023

    # =========================================================================
    # T024: Message Round-Trip
    # =========================================================================

    @testset "T024: Message encode/decode round-trip" begin

        @testset "Round-trip small message" begin
            original = UInt8[0x01, 0x02, 0x03, 0x04, 0x05]
            encoded = TestUtils.build_grpc_message(original)
            _, decoded = TestUtils.parse_grpc_message(encoded)
            @test decoded == original
        end

        @testset "Round-trip empty message" begin
            original = UInt8[]
            encoded = TestUtils.build_grpc_message(original)
            _, decoded = TestUtils.parse_grpc_message(encoded)
            @test decoded == original
        end

        @testset "Round-trip compressed flag" begin
            original = UInt8[0xAA, 0xBB]
            encoded = TestUtils.build_grpc_message(original; compressed=true)
            compressed, decoded = TestUtils.parse_grpc_message(encoded)
            @test compressed
            @test decoded == original
        end

        @testset "Round-trip binary data" begin
            original = Vector{UInt8}(0:255)
            encoded = TestUtils.build_grpc_message(original)
            _, decoded = TestUtils.parse_grpc_message(encoded)
            @test decoded == original
        end

    end  # T024

    # =========================================================================
    # T025: Invalid Message Handling
    # =========================================================================

    @testset "T025: Invalid message handling" begin

        @testset "Message too short (< 5 bytes)" begin
            @test_throws Exception TestUtils.parse_grpc_message(UInt8[0x00])
            @test_throws Exception TestUtils.parse_grpc_message(UInt8[0x00, 0x00, 0x00])
        end

        @testset "Truncated message" begin
            # Header says 5 bytes but only 2 bytes of data
            msg = UInt8[0x00, 0x00, 0x00, 0x00, 0x05, 0x01, 0x02]
            @test_throws Exception TestUtils.parse_grpc_message(msg)
        end

        @testset "Conformance test cases for invalid messages" begin
            for (input, _, _, _, should_fail) in ConformanceData.MESSAGE_FRAME_TEST_CASES
                if should_fail
                    @test_throws Exception TestUtils.parse_grpc_message(input)
                end
            end
        end

    end  # T025

    @testset "T025b: Incremental buffering does not require full-buffer copies" begin

        @testset "Large chunked message becomes readable only after final frame" begin
            payload = Vector{UInt8}(codeunits(repeat("package Modelica.Blocks;\n", 16_384)))
            message = TestUtils.build_grpc_message(payload)

            stream = gRPCServer.HTTP2Stream(1, length(message) + 1024)
            gRPCServer.receive_headers!(stream, false)

            split_point = 16_384
            gRPCServer.receive_data!(stream, message[1:split_point], false)
            @test gRPCServer.has_complete_grpc_message(stream) == false
            @test gRPCServer.read_grpc_message!(stream) === nothing

            gRPCServer.receive_data!(stream, message[(split_point + 1):end], true)
            @test gRPCServer.has_complete_grpc_message(stream) == true
            decoded = gRPCServer.read_grpc_message!(stream)
            @test decoded == payload
        end

        @testset "Sequential chunked messages preserve unread remainder" begin
            payload_a = Vector{UInt8}(codeunits(repeat("source_text_a\n", 8_192)))
            payload_b = Vector{UInt8}(codeunits(repeat("source_text_b\n", 2_048)))
            message_a = TestUtils.build_grpc_message(payload_a)
            message_b = TestUtils.build_grpc_message(payload_b)
            combined = vcat(message_a, message_b)

            stream = gRPCServer.HTTP2Stream(3, length(combined) + 1024)
            gRPCServer.receive_headers!(stream, false)

            first_chunk = length(message_a) - 128
            second_chunk = length(message_a) + 256
            gRPCServer.receive_data!(stream, combined[1:first_chunk], false)
            @test gRPCServer.has_complete_grpc_message(stream) == false

            gRPCServer.receive_data!(stream, combined[(first_chunk + 1):second_chunk], false)
            @test gRPCServer.has_complete_grpc_message(stream) == true
            @test gRPCServer.read_grpc_message!(stream) == payload_a

            gRPCServer.receive_data!(stream, combined[(second_chunk + 1):end], true)
            @test gRPCServer.has_complete_grpc_message(stream) == true
            @test gRPCServer.read_grpc_message!(stream) == payload_b
            @test isempty(gRPCServer.peek_data(stream))
        end
    end

    @testset "T025c: Batched frame writes flush once per response group" begin
        frame_a = gRPCServer.settings_frame()
        frame_b = gRPCServer.ping_frame()

        io = CountingIO()
        gRPCServer.write_frames(io, [frame_a, frame_b])
        @test io.write_count == 2
        @test io.flush_count == 1

        io_single = CountingIO()
        gRPCServer.write_frame(io_single, frame_a)
        gRPCServer.write_frame(io_single, frame_b)
        @test io_single.write_count == 2
        @test io_single.flush_count == 2
    end

    # =========================================================================
    # T026: Compression Codec Integration
    # =========================================================================

    @testset "T026: Compression codec integration" begin

        @testset "GZIP compress/decompress" begin
            original = Vector{UInt8}("Hello, gRPC compression test! " ^ 10)
            compressed = gRPCServer.compress(original, CompressionCodec.GZIP)
            decompressed = gRPCServer.decompress(compressed, CompressionCodec.GZIP)
            @test decompressed == original
            @test length(compressed) < length(original)  # Should compress
        end

        @testset "DEFLATE compress/decompress" begin
            original = Vector{UInt8}("Test data for deflate compression")
            compressed = gRPCServer.compress(original, CompressionCodec.DEFLATE)
            decompressed = gRPCServer.decompress(compressed, CompressionCodec.DEFLATE)
            @test decompressed == original
        end

        @testset "IDENTITY codec is no-op" begin
            original = UInt8[0x01, 0x02, 0x03]
            @test gRPCServer.compress(original, CompressionCodec.IDENTITY) == original
            @test gRPCServer.decompress(original, CompressionCodec.IDENTITY) == original
        end

        @testset "Parse grpc-encoding header" begin
            @test gRPCServer.parse_codec("gzip") == CompressionCodec.GZIP
            @test gRPCServer.parse_codec("deflate") == CompressionCodec.DEFLATE
            @test gRPCServer.parse_codec("identity") == CompressionCodec.IDENTITY
            @test gRPCServer.parse_codec("unknown") === nothing
        end

        @testset "Parse grpc-accept-encoding header" begin
            codecs = gRPCServer.parse_accept_encoding("gzip, deflate, identity")
            @test CompressionCodec.GZIP in codecs
            @test CompressionCodec.DEFLATE in codecs
            @test CompressionCodec.IDENTITY in codecs
        end

        @testset "Negotiate compression" begin
            client_codecs = [CompressionCodec.GZIP, CompressionCodec.DEFLATE]
            server_codecs = [CompressionCodec.DEFLATE, CompressionCodec.GZIP]
            result = gRPCServer.negotiate_compression(client_codecs, server_codecs)
            @test result == CompressionCodec.GZIP  # First match from client preference

            # No common codec
            empty_server = CompressionCodec.T[]
            result = gRPCServer.negotiate_compression(client_codecs, empty_server)
            @test result == CompressionCodec.IDENTITY  # Fallback
        end

    end  # T026

    # =========================================================================
    # T026b: read_grpc_message! with compression (coverage)
    # =========================================================================

    @testset "T026b: read_grpc_message! decompression" begin

        @testset "Decompress gzip message" begin
            # Create stream with grpc-encoding header
            stream = gRPCServer.HTTP2Stream(UInt32(1))
            stream.request_headers = [
                (":method", "POST"),
                (":path", "/test/Method"),
                ("content-type", "application/grpc"),
                ("grpc-encoding", "gzip"),
            ]

            # Create compressed message
            original = Vector{UInt8}("Hello, gRPC!")
            compressed_data = gRPCServer.compress(original, CompressionCodec.GZIP)

            # Build length-prefixed message with compressed flag = 1
            msg = vcat(
                UInt8[0x01],  # Compressed flag
                reinterpret(UInt8, [hton(UInt32(length(compressed_data)))]),
                compressed_data
            )

            # Write to stream buffer
            write(stream.data_buffer, msg)

            # Read and verify decompression
            result = gRPCServer.read_grpc_message!(stream)
            @test result !== nothing
            @test result == original
        end

        @testset "Decompress deflate message" begin
            stream = gRPCServer.HTTP2Stream(UInt32(1))
            stream.request_headers = [
                (":method", "POST"),
                (":path", "/test/Method"),
                ("content-type", "application/grpc"),
                ("grpc-encoding", "deflate"),
            ]

            original = Vector{UInt8}("Deflate test data")
            compressed_data = gRPCServer.compress(original, CompressionCodec.DEFLATE)

            msg = vcat(
                UInt8[0x01],
                reinterpret(UInt8, [hton(UInt32(length(compressed_data)))]),
                compressed_data
            )
            write(stream.data_buffer, msg)

            result = gRPCServer.read_grpc_message!(stream)
            @test result !== nothing
            @test result == original
        end

        @testset "Identity encoding with compressed flag" begin
            stream = gRPCServer.HTTP2Stream(UInt32(1))
            stream.request_headers = [
                (":method", "POST"),
                (":path", "/test/Method"),
                ("content-type", "application/grpc"),
                ("grpc-encoding", "identity"),
            ]

            original = UInt8[0x01, 0x02, 0x03]
            msg = vcat(
                UInt8[0x01],  # Compressed flag set
                reinterpret(UInt8, [hton(UInt32(length(original)))]),
                original
            )
            write(stream.data_buffer, msg)

            # With identity encoding, data should pass through unchanged
            result = gRPCServer.read_grpc_message!(stream)
            @test result !== nothing
            @test result == original
        end

        @testset "Compressed flag set but no grpc-encoding header" begin
            stream = gRPCServer.HTTP2Stream(UInt32(1))
            stream.request_headers = [
                (":method", "POST"),
                (":path", "/test/Method"),
                ("content-type", "application/grpc"),
                # No grpc-encoding header
            ]

            data = UInt8[0x01, 0x02, 0x03]
            msg = vcat(
                UInt8[0x01],  # Compressed flag set
                reinterpret(UInt8, [hton(UInt32(length(data)))]),
                data
            )
            write(stream.data_buffer, msg)

            # Should warn but still return data
            result = gRPCServer.read_grpc_message!(stream)
            @test result !== nothing
            @test result == data  # Returns as-is since no encoding specified
        end

        @testset "Unknown encoding codec" begin
            stream = gRPCServer.HTTP2Stream(UInt32(1))
            stream.request_headers = [
                (":method", "POST"),
                (":path", "/test/Method"),
                ("content-type", "application/grpc"),
                ("grpc-encoding", "unknown-codec"),
            ]

            data = UInt8[0x01, 0x02, 0x03]
            msg = vcat(
                UInt8[0x01],
                reinterpret(UInt8, [hton(UInt32(length(data)))]),
                data
            )
            write(stream.data_buffer, msg)

            # Unknown codec should return data unchanged
            result = gRPCServer.read_grpc_message!(stream)
            @test result !== nothing
            @test result == data
        end

        @testset "Uncompressed message (flag = 0)" begin
            stream = gRPCServer.HTTP2Stream(UInt32(1))
            stream.request_headers = [
                (":method", "POST"),
                (":path", "/test/Method"),
                ("content-type", "application/grpc"),
                ("grpc-encoding", "gzip"),  # Header present but flag is 0
            ]

            data = UInt8[0xAA, 0xBB, 0xCC]
            msg = vcat(
                UInt8[0x00],  # Not compressed
                reinterpret(UInt8, [hton(UInt32(length(data)))]),
                data
            )
            write(stream.data_buffer, msg)

            # Should not attempt decompression
            result = gRPCServer.read_grpc_message!(stream)
            @test result !== nothing
            @test result == data
        end

    end  # T026b

end  # AC3: Message Encoding
