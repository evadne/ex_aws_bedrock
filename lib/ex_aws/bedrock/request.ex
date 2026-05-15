defmodule ExAws.Bedrock.Request do
  @moduledoc """
  Perform AWS requests signed with the correct service.

  Actions on Amazon Bedrock Runtime need to be signed with Bedrock.
  Actions on Amazon Bedrock Powered by AWS Mantle need the Mantle host and
  the `bedrock-mantle` signing service.
  """

  @mantle_host {"region", "bedrock-mantle.region.api.aws"}

  @doc """
  Perform an AWS request with correct service.

  See `ExAws.request/2`.
  """
  def request(op, config_overrides \\ []),
    do: ExAws.request(op, check_service_override(op, config_overrides))

  @doc """
  Perform an AWS request with correct service, raise if it fails.

  See `ExAws.request!/2`.
  """
  def request!(op, config_overrides \\ []),
    do: ExAws.request!(op, check_service_override(op, config_overrides))

  @doc """
  Return a stream for the AWS resource.

  See `ExAws.stream!/2`.
  """
  def stream!(op, config_overrides \\ []),
    do: ExAws.stream!(op, check_service_override(op, config_overrides))

  defp check_service_override(%{service: :"bedrock-runtime"}, config_overrides),
    do: [{:service_override, :bedrock} | config_overrides]

  defp check_service_override(%{service: :bedrock, path: "/v1/" <> _}, config_overrides),
    do: mantle_config(config_overrides)

  defp check_service_override(
         %{service: :bedrock, path: "/anthropic/v1/" <> _},
         config_overrides
       ),
       do: mantle_config(config_overrides)

  defp check_service_override(_, config_overrides), do: config_overrides

  defp mantle_config(config_overrides) do
    [
      scheme: "https",
      host: @mantle_host,
      service_override: :"bedrock-mantle"
    ] ++ config_overrides
  end
end
