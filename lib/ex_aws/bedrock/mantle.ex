defmodule ExAws.Bedrock.Mantle do
  @moduledoc """
  Operations for Amazon Bedrock Powered by AWS Mantle.

  Mantle lives on `bedrock-mantle.<region>.api.aws` and exposes
  OpenAI-compatible Chat Completions / Responses surfaces plus an
  Anthropic-compatible Messages surface. Every constructor in this module
  returns an `ExAws.Operation.BedrockMantle` — a dedicated operation type
  that handles the Mantle wire shape: empty body on GETs, correct
  `x-amz-content-sha256` header, Mantle host rewriting, `bedrock-mantle`
  SigV4 signing, and OpenAI/Anthropic-shaped response parsing (no
  AWS-`__type`-envelope assumption).

  Stock Bedrock control-plane and Bedrock Runtime operations
  (`ExAws.Bedrock.*`) continue to use `ExAws.Operation.JSON` — those
  endpoints really are AWS JSON.
  """

  alias ExAws.Bedrock.Mantle.SSE

  @json_request_headers [{"Content-Type", "application/json"}]
  @stream_request_headers [{"accept", "text/event-stream"} | @json_request_headers]
  @anthropic_version "2023-06-01"

  @doc """
  List models available from the Mantle OpenAI-compatible API.

  [AWS User Guide](https://docs.aws.amazon.com/bedrock/latest/userguide/bedrock-mantle.html)
  """
  def list_models do
    %ExAws.Operation.BedrockMantle{
      http_method: :get,
      path: "/v1/models",
      service: :bedrock
    }
  end

  @doc """
  Create an OpenAI-compatible Chat Completions request on Mantle.

  Pass the same JSON body you would send to `/v1/chat/completions`. For
  streaming requests, set `"stream" => true` in the body and call
  `ExAws.Bedrock.stream!/2` with the returned operation.
  """
  def chat_completion(body) when is_map(body) or is_struct(body) do
    operation(:post, "/v1/chat/completions", body, @stream_request_headers)
  end

  @doc """
  Create an OpenAI-compatible Responses request on Mantle.

  Pass the same JSON body you would send to `/v1/responses`. For streaming
  requests, set `"stream" => true` in the body and call
  `ExAws.Bedrock.stream!/2` with the returned operation.
  """
  def response(body) when is_map(body) or is_struct(body) do
    operation(:post, "/v1/responses", body, @stream_request_headers)
  end

  @doc """
  Create an Anthropic-compatible Messages request on Mantle.

  Mantle requires the standard Anthropic API version header for this endpoint.
  For streaming requests, set `"stream" => true` in the body and call
  `ExAws.Bedrock.stream!/2` with the returned operation.
  """
  def message(body) when is_map(body) or is_struct(body) do
    operation(:post, "/anthropic/v1/messages", body, anthropic_headers())
  end

  defp operation(method, path, body, headers) do
    post = %ExAws.Operation.BedrockMantle{
      data: body,
      headers: headers,
      http_method: method,
      path: path,
      service: :bedrock
    }

    %{post | stream_builder: &SSE.stream_raw!(post, nil, &1)}
  end

  defp anthropic_headers do
    [{"anthropic-version", @anthropic_version} | @stream_request_headers]
  end
end
