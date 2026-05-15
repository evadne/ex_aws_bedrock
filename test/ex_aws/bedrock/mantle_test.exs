defmodule ExAws.Bedrock.MantleTest do
  use ExUnit.Case, async: true

  alias ExAws.Bedrock
  alias ExAws.Bedrock.Mantle
  alias ExAws.Operation.JSON

  defmodule CaptureClient do
    def request(method, url, body, headers, _http_opts) do
      send(self(), {:request, method, url, body, headers})
      {:ok, %{status_code: 200, body: ~s({"ok":true}), headers: []}}
    end
  end

  describe "list_models/0" do
    test "builds a Mantle OpenAI-compatible models request" do
      assert %JSON{
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

      assert %JSON{
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

      assert %JSON{
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

      assert %JSON{
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

  describe "Bedrock.request/2" do
    test "routes Mantle operations through the Mantle host and signing service" do
      request = Mantle.chat_completion(%{"model" => "openai.gpt-oss-120b", "messages" => []})

      assert {:ok, %{"ok" => true}} = Bedrock.request(request, ex_aws_config())

      assert_received {:request, :post,
                       "https://bedrock-mantle.us-east-1.api.aws/v1/chat/completions", body,
                       headers}

      assert %{"model" => "openai.gpt-oss-120b", "messages" => []} = Jason.decode!(body)
      assert {"host", "bedrock-mantle.us-east-1.api.aws"} in headers

      assert {"Authorization", authorization} = List.keyfind(headers, "Authorization", 0)
      assert authorization =~ "/us-east-1/bedrock-mantle/aws4_request"
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
