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
    def stream_raw!(
          %{service: service, data: data, headers: headers} = post_operation,
          _opts,
          config
        ) do
      encoded_data = config[:json_codec].encode!(data)
      url = build_request_url(post_operation, config)
      headers = [{"user-agent", @user_agent} | headers]

      {:ok, full_headers} =
        ExAws.Auth.headers(
          :post,
          url,
          service,
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
      verify_header!(headers, "Content-Type", @content_type)
    end

    defp verify_header!(headers, header, expected) do
      case Enum.find(headers, fn {name, _value} ->
             String.downcase(name) == String.downcase(header)
           end) do
        {_, ^expected} ->
          true

        {_, content_type} ->
          raise ExAws.Error, "Accepts #{expected}, received #{to_string(content_type)}"

        nil ->
          raise ExAws.Error, "Accepts #{expected}, received no #{header} header"
      end
    end
  else
    def stream_raw!(_, _, _) do
      raise "Mantle response streaming requires hackney in your mix dependencies"
    end
  end
end
