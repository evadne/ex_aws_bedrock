defmodule ExAws.Operation.BedrockMantle do
  @moduledoc """
  Operation struct for AWS Bedrock Powered by AWS Mantle.

  Mantle sits on `bedrock-mantle.<region>.api.aws` and exposes OpenAI-compatible
  Chat Completions / Responses surfaces plus an Anthropic-compatible Messages
  surface. The wire is JSON only incidentally: it is JSON because OpenAI and
  Anthropic happen to be JSON APIs, not because Mantle is itself an AWS JSON
  service. This operation type owns the resulting differences from
  `ExAws.Operation.JSON`:

    * **Body shape.** Mantle's `GET /v1/models` carries no body at all; AWS
      SigV4 expects `sha256("")` as the content hash for that case.
      `Operation.JSON` instead produces a phantom `"{}"` body from the default
      `data: %{}` — fine for AWS JSON services whose surface is RPC-shaped
      Actions, wrong for a REST-shaped GET like Mantle's. This module sends
      an empty body for `:get`/`:head`/`:delete`, the caller's binary body
      verbatim, or `config[:json_codec].encode!(data)` for map/struct payloads
      on POST/PUT.

    * **Correct `x-amz-content-sha256`.** Stamped with `hex(sha256(body))` of
      the wire body. Mantle strictly enforces this (`SHA256 of body '…' does
      not match X-Amz-Content-Sha256 header '…'`) — the AWS-documented SigV4
      contract that `Operation.JSON` does not honour upstream as of 2026-05.

    * **Mantle host and signing service.** The request is rewritten to
      `bedrock-mantle.<region>.api.aws` and signed with the `bedrock-mantle`
      SigV4 service name (the operation's `service` field). `ExAws.Bedrock.Request`
      dispatches BedrockMantle ops through `ExAws.Config.new(:bedrock, …)` to
      get partition-safe defaults — `bedrock-mantle` is not in upstream ex_aws's
      `priv/endpoints.exs` as of 2026-05 — and this operation's `perform/2` /
      `stream!/2` then rewrite host/scheme/port for Mantle.

    * **Response body shape.** Mantle returns OpenAI- or
      Anthropic-shaped JSON on success and OpenAI-shaped error envelopes
      (`{"error":{"code":...,"message":...,"type":...}}`) on failure. These
      are not AWS's `__type`/`message` error shapes. The parse path here
      decodes the body with `config[:json_codec]` and returns whatever shape
      came back, without pretending to know the AWS envelope.

  Stock Bedrock control-plane and Bedrock Runtime continue to use
  `ExAws.Operation.JSON` because those endpoints really are AWS JSON.
  """

  alias ExAws.Auth.Utils

  defstruct stream_builder: nil,
            parser: &Function.identity/1,
            error_parser: &Function.identity/1,
            before_request: nil,
            http_method: :post,
            path: "/",
            data: nil,
            params: %{},
            headers: [],
            service: :"bedrock-mantle"

  @type t :: %__MODULE__{
          stream_builder: (any -> Enumerable.t()) | nil,
          parser: (any -> any),
          error_parser: (any -> any),
          before_request: (t, map -> t) | nil,
          http_method: :get | :post | :put | :delete | :head,
          path: String.t(),
          data: map() | struct() | binary() | nil,
          params: map() | keyword(),
          headers: [{String.t(), String.t()}],
          service: :"bedrock-mantle"
        }

  @mantle_host_template "bedrock-mantle.region.api.aws"

  @doc """
  Prepend Mantle host/scheme defaults to a `config_overrides` keyword list
  before it is passed to `ExAws.Config.new/2`.

  The defaults come first in the keyword list so caller-supplied overrides
  (e.g. a Bypass `base_url` injected via `host:` / `scheme:` / `port:`) win
  on the subsequent `Map.new` merge inside `ExAws.Config.new/2`. This is the
  same pattern the prior `ExAws.Bedrock.Request.mantle_config/1` shim used
  and is what makes test-time mocking possible.

  The `host` is encoded as a `{stub, host}` tuple — `ExAws.Config.parse_host_for_region/1`
  substitutes the configured region into the template before signing.

  No `:service_override` is set: the operation's own `service` field is
  `:"bedrock-mantle"` and `ExAws.Auth.headers/6` uses it directly when no
  override is present.
  """
  def apply_routing(config_overrides) when is_list(config_overrides) do
    [
      scheme: "https",
      host: {"region", @mantle_host_template}
    ] ++ config_overrides
  end

  @doc """
  Compute the canonical wire body for the operation.

  Empty for `:get`, `:head`, `:delete` regardless of `:data` (Mantle's REST
  surface carries no body on those methods). Caller-supplied binary passed
  through verbatim. Map/struct payloads JSON-encoded via the configured
  codec.
  """
  def encode_body(%__MODULE__{http_method: method}, _config)
      when method in [:get, :head, :delete],
      do: ""

  def encode_body(%__MODULE__{data: nil}, _config), do: ""
  def encode_body(%__MODULE__{data: ""}, _config), do: ""
  def encode_body(%__MODULE__{data: data}, _config) when is_binary(data), do: data

  def encode_body(%__MODULE__{data: data}, config) do
    config[:json_codec].encode!(data)
  end

  @doc """
  Stamp `x-amz-content-sha256` and (where appropriate) `content-length` onto
  the operation's headers, given the canonical wire body. Replaces any
  caller-supplied values for these two headers.
  """
  def build_headers(%__MODULE__{} = operation, body) do
    hashed_payload = Utils.hash_sha256(body)

    operation.headers
    |> upsert_header("x-amz-content-sha256", hashed_payload)
    |> maybe_put_content_length(body, operation.http_method)
  end

  defp upsert_header(headers, name, value) do
    target = String.downcase(name)

    case Enum.find_index(headers, fn {k, _} -> String.downcase(to_string(k)) == target end) do
      nil -> [{name, value} | headers]
      idx -> List.replace_at(headers, idx, {name, value})
    end
  end

  defp maybe_put_content_length(headers, "", _method), do: headers

  defp maybe_put_content_length(headers, body, _method),
    do: upsert_header(headers, "content-length", Integer.to_string(IO.iodata_length(body)))
end

defimpl ExAws.Operation, for: ExAws.Operation.BedrockMantle do
  alias ExAws.Operation.BedrockMantle

  def perform(operation, config) do
    operation = handle_before_request(operation, config)
    url = ExAws.Request.Url.build(operation, config)
    body = BedrockMantle.encode_body(operation, config)
    headers = BedrockMantle.build_headers(operation, body)

    ExAws.Request.request(
      operation.http_method,
      url,
      body,
      headers,
      config,
      operation.service
    )
    |> operation.error_parser.()
    |> ExAws.Request.default_aws_error()
    |> parse(config)
  end

  def stream!(%BedrockMantle{stream_builder: nil}, _config) do
    raise ArgumentError, """
    This operation does not support streaming!
    """
  end

  def stream!(%BedrockMantle{stream_builder: stream_builder}, config) do
    stream_builder.(config)
  end

  defp handle_before_request(%BedrockMantle{before_request: nil} = op, _config), do: op
  defp handle_before_request(%BedrockMantle{before_request: cb} = op, config), do: cb.(op, config)

  # Mantle returns OpenAI- or Anthropic-shaped JSON; we decode it transparently
  # and hand the caller whatever Mantle sent. Errors flow through ExAws's
  # generic `:http_error` path (which preserves the raw response) because
  # Mantle's error envelope is OpenAI-shaped, not AWS's `__type`/`message`.
  defp parse({:error, result}, _config), do: {:error, result}
  defp parse({:ok, %{body: ""}}, _config), do: {:ok, %{}}

  defp parse({:ok, %{body: body}}, config) do
    {:ok, config[:json_codec].decode!(body)}
  end
end
