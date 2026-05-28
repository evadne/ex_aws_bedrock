defmodule ExAws.Bedrock.Mantle.SSE do
  @moduledoc """
  Raw Server-Sent Events streaming for Mantle operations.

  The stream intentionally yields raw response bytes. Mantle's OpenAI and
  Anthropic-compatible endpoints already speak SSE, so callers that proxy the
  same protocol can forward these chunks without decoding and reconstructing
  protocol events.
  """

  defdelegate build_request_url(post_operation, config), to: ExAws.Request.Url, as: :build

  @content_type "text/event-stream"

  if {:module, :hackney} == Code.ensure_loaded(:hackney) &&
       Kernel.function_exported?(:hackney, :post, 4) do
    @http_ua :hackney_request.default_ua()
    @library_version Application.spec(:ex_aws_bedrock)[:vsn]
    @user_agent "#{@http_ua} ex_aws/bedrock/#{@library_version}"
    @hackney_options [{:async, :once}]

    @doc """
    Stream raw SSE bytes from a Mantle response.
    """
    def stream_raw!(%ExAws.Operation.BedrockMantle{} = post_operation, _opts, config) do
      encoded_data = ExAws.Operation.BedrockMantle.encode_body(post_operation, config)
      url = build_request_url(post_operation, config)

      headers =
        post_operation
        |> ExAws.Operation.BedrockMantle.build_headers(encoded_data)
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

      request_fun = fn [] ->
        {:ok, ref} = :hackney.post(url, full_headers, encoded_data, @hackney_options)

        receive do
          {:hackney_response, ^ref, {:status, 200, _reason}} ->
            ref

          {:hackney_response, ^ref, {:status, status, reason}} ->
            {:error, status, reason}

          {:hackney_response, ^ref, {:error, {:closed, :timeout}}} ->
            :closed
        end
      end

      Stream.resource(
        fn -> request_fun.([]) end,
        fn
          :closed ->
            {:halt, []}

          {:error, status, reason} ->
            raise ExAws.Error, "#{to_string(status)}: #{to_string(reason)}"

          ref when is_reference(ref) ->
            :ok = :hackney.stream_next(ref)

            receive do
              {:hackney_response, ^ref, {:headers, headers}} ->
                verify_event_stream!(headers)
                {[], ref}

              {:hackney_response, ^ref, :done} ->
                {:halt, []}

              {:hackney_response, ^ref, data} ->
                {[data], ref}
            end
        end,
        &Function.identity/1
      )
    end

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
