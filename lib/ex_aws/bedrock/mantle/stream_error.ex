defmodule ExAws.Bedrock.Mantle.StreamError do
  @moduledoc """
  Structured failure returned while opening or consuming a Mantle SSE stream.

  HTTP failures preserve the upstream status, headers, and response body so a
  gateway can translate the error without discarding useful diagnostics.
  Transport failures carry the underlying Hackney reason and have no HTTP
  status.
  """

  defexception [:kind, :status, :headers, :response_body, :reason, :message]

  @type t :: %__MODULE__{
          kind: :http | :transport,
          status: non_neg_integer() | nil,
          headers: list(),
          response_body: binary() | nil,
          reason: term(),
          message: String.t()
        }

  @impl true
  def exception(opts) do
    kind = Keyword.fetch!(opts, :kind)
    status = Keyword.get(opts, :status)
    headers = Keyword.get(opts, :headers, [])
    response_body = Keyword.get(opts, :response_body)
    reason = Keyword.get(opts, :reason)

    %__MODULE__{
      kind: kind,
      status: status,
      headers: headers,
      response_body: response_body,
      reason: reason,
      message: message(kind, status, response_body, reason)
    }
  end

  defp message(:http, status, response_body, reason) do
    detail = present(response_body) || present(reason) || "unknown upstream error"
    "Mantle stream returned HTTP #{status}: #{detail}"
  end

  defp message(:transport, _status, _response_body, reason) do
    "Mantle stream transport failed: #{inspect(reason)}"
  end

  defp present(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      value -> value
    end
  end

  defp present(nil), do: nil
  defp present(value), do: inspect(value)
end
