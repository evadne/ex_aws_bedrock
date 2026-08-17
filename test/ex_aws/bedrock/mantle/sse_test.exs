defmodule ExAws.Bedrock.Mantle.SSETest do
  use ExUnit.Case, async: true

  alias ExAws.Bedrock
  alias ExAws.Bedrock.Mantle
  alias ExAws.Bedrock.Mantle.{SSE, StreamError}

  describe "hackney_options/1" do
    test "honours caller timeouts while retaining the streaming transport mode" do
      opts =
        SSE.hackney_options(%{
          http_opts: [
            async: false,
            protocols: [:http2],
            recv_timeout: 600_000,
            connect_timeout: 10_000
          ]
        })

      assert Keyword.get(opts, :async) == :once
      assert Keyword.get(opts, :protocols) == [:http1]
      assert Keyword.get(opts, :recv_timeout) == 600_000
      assert Keyword.get(opts, :connect_timeout) == 10_000
    end
  end

  describe "stream_raw!/3" do
    test "preserves a non-200 response status, headers, and body" do
      body = ~s({"error":{"type":"invalid_request_error","message":"temperature is unsupported"}})

      {port, server} =
        serve(fn socket ->
          send_response(socket, 400, "Bad Request", body,
            content_type: "application/json",
            extra_headers: [{"x-amzn-requestid", "request-123"}]
          )
        end)

      error =
        assert_raise StreamError, fn ->
          port
          |> stream(recv_timeout: 1_000)
          |> Enum.to_list()
        end

      assert error.kind == :http
      assert error.status == 400
      assert error.response_body == body
      assert error.reason == "Bad Request"
      assert header(error.headers, "x-amzn-requestid") == "request-123"
      assert Exception.message(error) =~ "temperature is unsupported"
      Task.await(server)
    end

    test "raises a typed transport error when the first SSE byte exceeds recv_timeout" do
      {port, server} =
        serve(fn socket ->
          :ok =
            :gen_tcp.send(
              socket,
              "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n" <>
                "transfer-encoding: chunked\r\nconnection: close\r\n\r\n"
            )

          Process.sleep(100)
        end)

      error =
        assert_raise StreamError, fn ->
          port
          |> stream(recv_timeout: 20)
          |> Enum.to_list()
        end

      assert error.kind == :transport
      assert error.status == nil
      assert error.reason in [:timeout, {:closed, :timeout}]
      Task.await(server)
    end

    test "waits for a delayed first SSE byte when recv_timeout permits it" do
      event = ~s(event: message_stop\ndata: {"type":"message_stop"}\n\n)

      {port, server} =
        serve(fn socket ->
          :ok =
            :gen_tcp.send(
              socket,
              "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n" <>
                "transfer-encoding: chunked\r\nconnection: close\r\n\r\n"
            )

          Process.sleep(50)
          :ok = :gen_tcp.send(socket, chunk(event) <> "0\r\n\r\n")
        end)

      assert port |> stream(recv_timeout: 500) |> Enum.to_list() == [event]
      Task.await(server)
    end
  end

  describe "exception/1" do
    test "keeps transport failures descriptive without claiming an HTTP response" do
      error = StreamError.exception(kind: :transport, reason: {:closed, :timeout})

      assert error.status == nil

      assert Exception.message(error) ==
               "Mantle stream transport failed: {:closed, :timeout}"
    end
  end

  defp stream(port, http_opts) do
    operation =
      Mantle.message(%{
        "model" => "anthropic.claude-opus-5",
        "messages" => [%{"role" => "user", "content" => "Hi"}],
        "max_tokens" => 16,
        "stream" => true
      })

    Bedrock.stream!(operation,
      access_key_id: "AKIAIOSFODNN7EXAMPLE",
      secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      region: "us-east-1",
      scheme: "http",
      host: "127.0.0.1",
      port: port,
      http_opts: http_opts
    )
  end

  defp serve(handler) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)

    task =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 1_000)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 1_000)

        try do
          handler.(socket)
        after
          :gen_tcp.close(socket)
          :gen_tcp.close(listener)
        end
      end)

    {port, task}
  end

  defp send_response(socket, status, reason, body, opts) do
    content_type = Keyword.fetch!(opts, :content_type)
    extra_headers = Keyword.get(opts, :extra_headers, [])

    header_pairs = [
      {"content-type", content_type},
      {"content-length", byte_size(body)},
      {"connection", "close"}
      | extra_headers
    ]

    headers = Enum.map_join(header_pairs, "", fn {name, value} -> "#{name}: #{value}\r\n" end)

    :gen_tcp.send(socket, "HTTP/1.1 #{status} #{reason}\r\n#{headers}\r\n#{body}")
  end

  defp chunk(data) do
    size = Integer.to_string(byte_size(data), 16)
    "#{size}\r\n#{data}\r\n"
  end

  defp header(headers, expected_name) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(to_string(name)) == expected_name, do: to_string(value)
    end)
  end
end
