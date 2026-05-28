defmodule ExAws.Bedrock.Request do
  @moduledoc """
  Perform AWS requests signed with the correct service.

  Actions on Amazon Bedrock Runtime need to be signed with the `bedrock`
  service name (matching the `credentialScope` in the AWS partition data),
  even though the HTTP host is `bedrock-runtime.<region>.amazonaws.com`.
  This module injects that override at request time.

  Actions on Amazon Bedrock Powered by AWS Mantle are constructed as
  `ExAws.Operation.BedrockMantle` structs; that operation type owns its own
  host rewriting and `bedrock-mantle` signing service, so no override is
  needed here for the Mantle path.
  """

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

  defp check_service_override(%ExAws.Operation.JSON{service: :"bedrock-runtime"}, config_overrides),
    do: [{:service_override, :bedrock} | config_overrides]

  defp check_service_override(_op, config_overrides), do: config_overrides
end
