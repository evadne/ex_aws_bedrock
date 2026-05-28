defmodule ExAws.Bedrock.MantleTest do
  use ExUnit.Case, async: true

  alias ExAws.Bedrock
  alias ExAws.Bedrock.Mantle
  alias ExAws.Operation.BedrockMantle

  defmodule CaptureClient do
    def request(method, url, body, headers, _http_opts) do
      send(self(), {:request, method, url, body, headers})
      {:ok, %{status_code: 200, body: ~s({"ok":true}), headers: []}}
    end
  end

  describe "list_models/0" do
    test "builds a Mantle OpenAI-compatible models request" do
      assert %BedrockMantle{
               http_method: :get,
               path: "/v1/models",
               service: :bedrock,
               stream_builder: nil
             } = Mantle.list_models()
    end
  end

  describe "chat_completion/1" do
    test "builds a Mantle Chat Completions request" do
      request = Mantle.chat_completion(%{"model" => "openai.gpt-oss-120b"})

      assert %BedrockMantle{
               data: %{"model" => "openai.gpt-oss-120b"},
               http_method: :post,
               path: "/v1/chat/completions",
               service: :bedrock,
               stream_builder: stream_builder
             } = request

      assert is_function(stream_builder, 1)
      assert {"accept", "text/event-stream"} in request.headers
      assert {"Content-Type", "application/json"} in request.headers
    end
  end

  describe "response/1" do
    test "builds a Mantle Responses request" do
      request = Mantle.response(%{"model" => "openai.gpt-oss-120b"})

      assert %BedrockMantle{
               data: %{"model" => "openai.gpt-oss-120b"},
               http_method: :post,
               path: "/v1/responses",
               service: :bedrock,
               stream_builder: stream_builder
             } = request

      assert is_function(stream_builder, 1)
      assert {"accept", "text/event-stream"} in request.headers
      assert {"Content-Type", "application/json"} in request.headers
    end
  end

  describe "message/1" do
    test "builds a Mantle Anthropic-compatible Messages request" do
      request = Mantle.message(%{"model" => "anthropic.claude-opus-4-7"})

      assert %BedrockMantle{
               data: %{"model" => "anthropic.claude-opus-4-7"},
               http_method: :post,
               path: "/anthropic/v1/messages",
               service: :bedrock,
               stream_builder: stream_builder
             } = request

      assert is_function(stream_builder, 1)
      assert {"accept", "text/event-stream"} in request.headers
      assert {"anthropic-version", "2023-06-01"} in request.headers
      assert {"Content-Type", "application/json"} in request.headers
    end
  end

  describe "Bedrock.request/2 — Mantle routing" do
    test "POST chat_completion: Mantle host, bedrock-mantle signing, correct sha256" do
      payload = %{"model" => "openai.gpt-oss-120b", "messages" => []}
      request = Mantle.chat_completion(payload)

      assert {:ok, %{"ok" => true}} = Bedrock.request(request, ex_aws_config())

      assert_received {:request, :post,
                       "https://bedrock-mantle.us-east-1.api.aws/v1/chat/completions", body,
                       headers}

      assert ^payload = Jason.decode!(body)
      assert {"host", "bedrock-mantle.us-east-1.api.aws"} in headers

      assert {"Authorization", authorization} = List.keyfind(headers, "Authorization", 0)
      assert authorization =~ "/us-east-1/bedrock-mantle/aws4_request"

      # AWS-spec-correct content hash: equals sha256 of the actual wire body,
      # never the literal "" that ExAws.Operation.JSON would inject.
      assert {"x-amz-content-sha256", hash} =
               List.keyfind(headers, "x-amz-content-sha256", 0)

      assert hash == ExAws.Auth.Utils.hash_sha256(body)
      refute hash == ""
    end

    test "GET list_models: no body, content hash is sha256(\"\")" do
      assert {:ok, %{"ok" => true}} =
               Bedrock.request(Mantle.list_models(), ex_aws_config())

      assert_received {:request, :get,
                       "https://bedrock-mantle.us-east-1.api.aws/v1/models", body, headers}

      assert body == "", "GET to Mantle /v1/models must carry no body"

      assert {"x-amz-content-sha256", hash} =
               List.keyfind(headers, "x-amz-content-sha256", 0)

      assert hash == ExAws.Auth.Utils.hash_sha256(""),
             "GET content hash must be sha256(\"\") = e3b0c4… not sha256(\"{}\")"

      # No content-length on an empty-body GET — RFC 7230 doesn't require it
      # and we don't synthesise it.
      refute List.keyfind(headers, "content-length", 0)
    end

    test "POST anthropic/v1/messages: Mantle host + anthropic-version header preserved" do
      payload = %{"model" => "anthropic.claude-opus-4-7", "messages" => [], "max_tokens" => 16}
      request = Mantle.message(payload)

      assert {:ok, %{"ok" => true}} = Bedrock.request(request, ex_aws_config())

      assert_received {:request, :post,
                       "https://bedrock-mantle.us-east-1.api.aws/anthropic/v1/messages", body,
                       headers}

      assert ^payload = Jason.decode!(body)
      assert {"anthropic-version", "2023-06-01"} in headers

      assert {"x-amz-content-sha256", hash} =
               List.keyfind(headers, "x-amz-content-sha256", 0)

      assert hash == ExAws.Auth.Utils.hash_sha256(body)
    end
  end

  defp ex_aws_config do
    [
      access_key_id: "AKIAIOSFODNN7EXAMPLE",
      secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      region: "us-east-1",
      http_client: CaptureClient,
      json_codec: Jason
    ]
  end
end
