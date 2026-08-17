defmodule ExAws.Bedrock.Mantle.SSE do
  @moduledoc """
  Raw Server-Sent Events streaming for Mantle operations.

  The stream intentionally yields raw response bytes. Mantle's OpenAI and
  Anthropic-compatible endpoints already speak SSE, so callers that proxy the
  same protocol can forward these chunks without decoding and reconstructing
  protocol events.
  """

  defdelegate build_request_url(post_operation, config), to: ExAws.Request.Url, as: :build

  alias ExAws.Bedrock.Mantle.StreamError
  alias ExAws.Operation.BedrockMantle, as: BedrockMantleOperation

  @content_type "text/event-stream"

  if {:module, :hackney} == Code.ensure_loaded(:hackney) &&
       Kernel.function_exported?(:hackney, :post, 4) do
    if function_exported?(:hackney, :default_ua, 0) do
      @http_ua :hackney.default_ua()
    else
      @http_ua :hackney_request.default_ua()
    end

    @library_version Application.spec(:ex_aws_bedrock)[:vsn]
    @user_agent "#{@http_ua} ex_aws/bedrock/#{@library_version}"
    @hackney_options [{:async, :once}, {:protocols, [:http1]}]

    @doc """
    Stream raw SSE bytes from a Mantle response.
    """
    def stream_raw!(%BedrockMantleOperation{} = post_operation, _opts, config) do
      encoded_data = BedrockMantleOperation.encode_body(post_operation, config)
      url = build_request_url(post_operation, config)

      headers =
        post_operation
        |> BedrockMantleOperation.build_headers(encoded_data)
        |> List.keystore("user-agent", 0, {"user-agent", @user_agent})

      {:ok, full_headers} =
        ExAws.Auth.headers(
          :post,
          url,
          post_operation.service,
          config,
          headers,
          encoded_data
        )

      hackney_opts = hackney_options(config)

      request_fun = fn [] ->
        {:ok, ref} = :hackney.post(url, full_headers, encoded_data, hackney_opts)

        receive do
          {:hackney_response, ^ref, {:status, 200, _reason}} ->
            {:streaming, ref}

          {:hackney_response, ^ref, {:status, status, reason}} ->
            {:http_error, ref, status, reason, [], []}

          {:hackney_response, ^ref, {:error, reason}} ->
            raise_transport_error(ref, reason)
        end
      end

      Stream.resource(
        fn -> request_fun.([]) end,
        fn
          {:streaming, ref} ->
            :ok = :hackney.stream_next(ref)

            receive do
              {:hackney_response, ^ref, {:headers, headers}} ->
                verify_event_stream!(headers)
                {[], {:streaming, ref}}

              {:hackney_response, ^ref, :done} ->
                {:halt, {:done, ref}}

              {:hackney_response, ^ref, {:error, reason}} ->
                raise_transport_error(ref, reason)

              {:hackney_response, ^ref, data} when is_binary(data) ->
                {[data], {:streaming, ref}}

              {:hackney_response, ^ref, other} ->
                raise_transport_error(ref, {:unexpected_message, other})
            end

          {:http_error, ref, status, reason, headers, body} ->
            :ok = :hackney.stream_next(ref)

            receive do
              {:hackney_response, ^ref, {:headers, headers}} ->
                {[], {:http_error, ref, status, reason, headers, body}}

              {:hackney_response, ^ref, :done} ->
                raise_http_error(ref, status, reason, headers, body)

              {:hackney_response, ^ref, {:error, transport_reason}} ->
                raise_http_error(ref, status, transport_reason, headers, body)

              {:hackney_response, ^ref, data} when is_binary(data) ->
                {[], {:http_error, ref, status, reason, headers, [data | body]}}

              {:hackney_response, ^ref, other} ->
                raise_http_error(ref, status, {:unexpected_message, other}, headers, body)
            end
        end,
        &close/1
      )
    end

    @doc false
    def hackney_options(config) do
      config
      |> Map.get(:http_opts, [])
      |> Keyword.merge(@hackney_options)
    end

    defp raise_http_error(ref, status, reason, headers, body) do
      close(ref)

      raise StreamError,
        kind: :http,
        status: status,
        headers: headers,
        response_body: body |> Enum.reverse() |> IO.iodata_to_binary(),
        reason: reason
    end

    defp raise_transport_error(ref, reason) do
      close(ref)
      raise StreamError, kind: :transport, reason: reason
    end

    defp close({state, ref}) when state in [:streaming, :done], do: close(ref)
    defp close({:http_error, ref, _status, _reason, _headers, _body}), do: close(ref)

    defp close(ref) when is_reference(ref) or is_pid(ref) do
      :hackney.close(ref)
      :ok
    catch
      :exit, _reason -> :ok
    end

    defp close(_state), do: :ok

    defp verify_event_stream!(headers) do
      verify_content_type!(headers, @content_type)
    end

    # Compare media type only, ignoring parameters (e.g. `; charset=utf-8`).
    # Mantle's Anthropic-compatible Messages surface returns
    # `text/event-stream; charset=utf-8` while the OpenAI-compatible Chat and
    # Responses surfaces return `text/event-stream` without the charset
    # parameter — both are valid `text/event-stream` content types per
    # RFC 7231 §3.1.1, and rejecting the parameterised form was a strict
    # pattern-match bug in earlier versions of this verifier.
    defp verify_content_type!(headers, expected) do
      case Enum.find(headers, fn {name, _value} ->
             String.downcase(to_string(name)) == "content-type"
           end) do
        nil ->
          raise ExAws.Error, "Accepts #{expected}, received no Content-Type header"

        {_, value} ->
          received = value |> to_string() |> String.split(";", parts: 2) |> hd() |> String.trim()

          if String.downcase(received) == String.downcase(expected) do
            true
          else
            raise ExAws.Error, "Accepts #{expected}, received #{value}"
          end
      end
    end
  else
    def stream_raw!(_, _, _) do
      raise "Mantle response streaming requires hackney in your mix dependencies"
    end
  end
end
