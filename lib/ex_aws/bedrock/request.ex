defmodule ExAws.Bedrock.Request do
  @moduledoc """
  Perform AWS Bedrock requests with the correct service and routing.

  Three operation flavours, dispatched by struct type:

    * **`ExAws.Operation.BedrockMantle`** (Mantle): the operation's `service`
      field is `:"bedrock-mantle"` — semantically accurate, used as the
      SigV4 signing service name in `ExAws.Auth.headers/6`. However,
      `bedrock-mantle` is not yet in upstream ex_aws's `priv/endpoints.exs`
      (as of 2026-05), so we cannot pass it to `ExAws.Config.new/2` — the
      partition lookup would crash. We build the config from `:bedrock`
      (a partition-safe key) and let the operation's `perform/2` rewrite
      host/scheme/port for Mantle.

    * **`ExAws.Operation.JSON` with `service: :"bedrock-runtime"`** (Bedrock
      Runtime): the HTTP host is `bedrock-runtime.<region>.amazonaws.com`
      but the SigV4 `credentialScope.service` is `bedrock` (per partition
      data). We inject `service_override: :bedrock` into the config so
      `ExAws.Auth.headers/6` signs accordingly.

    * **`ExAws.Operation.JSON` with `service: :bedrock`** (Bedrock control
      plane): host and signing both `:bedrock`; nothing to override.
  """

  alias ExAws.Operation.BedrockMantle

  @doc """
  Perform an AWS Bedrock request.

  See `ExAws.request/2`.
  """
  def request(op, config_overrides \\ [])

  def request(%BedrockMantle{} = op, opts) do
    ExAws.Operation.perform(op, mantle_config(opts))
  end

  def request(op, opts), do: ExAws.request(op, check_service_override(op, opts))

  @doc """
  Perform an AWS Bedrock request, raise if it fails.

  See `ExAws.request!/2`.
  """
  def request!(op, config_overrides \\ [])

  def request!(%BedrockMantle{} = op, opts) do
    case request(op, opts) do
      {:ok, result} -> result
      {:error, error} -> raise ExAws.Error, message(error)
    end
  end

  def request!(op, opts), do: ExAws.request!(op, check_service_override(op, opts))

  @doc """
  Return a stream for the AWS Bedrock resource.

  See `ExAws.stream!/2`.
  """
  def stream!(op, config_overrides \\ [])

  def stream!(%BedrockMantle{} = op, opts) do
    ExAws.Operation.stream!(op, mantle_config(opts))
  end

  def stream!(op, opts), do: ExAws.stream!(op, check_service_override(op, opts))

  defp check_service_override(
         %ExAws.Operation.JSON{service: :"bedrock-runtime"},
         config_overrides
       ),
       do: [{:service_override, :bedrock} | config_overrides]

  defp check_service_override(_op, config_overrides), do: config_overrides

  # Build a fully-resolved ExAws config for a BedrockMantle op. `apply_routing`
  # prepends Mantle host/scheme defaults; caller-supplied overrides (e.g. a
  # Bypass `base_url` for tests) come later in the keyword list and win on
  # `ExAws.Config.new/2`'s internal `Map.new` merge.
  defp mantle_config(opts) do
    opts
    |> BedrockMantle.apply_routing()
    |> then(&ExAws.Config.new(:bedrock, &1))
  end

  defp message(error) when is_binary(error), do: error
  defp message(error), do: inspect(error)
end
