defmodule Finch.ALPNIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  @pool_opts [
    protocols: [:http1, :http2],
    conn_opts: [transport_opts: [verify: :verify_none]]
  ]

  setup_all do
    {:ok, _} = Finch.ALPNServer.start(0)
    {:ok, _} = Finch.ALPNServer.start(0, protocols: ["http/1.1"], ref: Finch.ALPNServer.HTTP1)

    # A port nobody listens on
    {:ok, socket} = :ssl.listen(0, mode: :binary)
    {:ok, {_address, closed_port}} = :ssl.sockname(socket)
    :ssl.close(socket)

    {:ok,
     url: "https://localhost:#{:ranch.get_port(Finch.ALPNServer)}",
     http1_url: "https://localhost:#{:ranch.get_port(Finch.ALPNServer.HTTP1)}",
     closed_url: "https://localhost:#{closed_port}"}
  end

  # The test is a listener of the Finch registry: pools show up as they
  # register and factorys leave as they hand over. Finch is started with the
  # pools of the `pools` tag, configured with `pool_opts` on top of @pool_opts.
  setup %{test: test, url: url} = context do
    listener = :"#{test} listener"
    Process.register(self(), listener)
    :telemetry_test.attach_event_handlers(self(), [[:finch, :connect, :start]])

    pool_opts = Keyword.merge(@pool_opts, context[:pool_opts] || [])

    pools =
      case context[:pools] do
        nil -> %{}
        :default -> %{default: pool_opts}
        :url -> %{url => pool_opts}
      end

    start_supervised!({Finch, name: test, registry_listeners: [listener], pools: pools})

    # A pool strategy preferring shards that have not negotiated the protocol yet
    negotiating = fn entries ->
      Enum.find(entries, hd(entries), &match?({_pid, Finch.ALPN.Pool}, &1))
    end

    {:ok,
     finch_name: test,
     pool_name: Finch.Pool.to_name(Finch.Pool.new(url)),
     negotiating: negotiating}
  end

  describe "protocols: [:http1, :http2]" do
    @describetag pools: :default

    test "uses the HTTP/2 pool when the server negotiates h2", %{
      url: url,
      finch_name: finch_name,
      pool_name: pool_name
    } do
      # Bodies larger than the initial HTTP/2 window failed when HTTP/2
      # connections were used by the HTTP/1 pool (issue #265)
      body = :crypto.strong_rand_bytes(65_538)
      request = Finch.build(:post, url <> "/echo", [], body)

      assert {:ok, %{status: 200} = response} = Finch.request(request, finch_name)
      assert Jason.decode!(response.body)["received_bytes"] == 65_538
      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)

      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      refute_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP1.Pool}

      # The connection made to negotiate the protocol is the one the pool uses
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      refute_receive {[:finch, :connect, :start], _ref, _measurements, _metadata}, 100
    end

    test "uses the HTTP/1 pool when the server negotiates http/1.1", %{
      http1_url: url,
      finch_name: finch_name
    } do
      pool_name = Finch.Pool.to_name(Finch.Pool.new(url))
      request = Finch.build(:post, url <> "/echo", [], :crypto.strong_rand_bytes(65_538))

      assert {:ok, %{status: 200} = response} = Finch.request(request, finch_name)
      assert Jason.decode!(response.body)["received_bytes"] == 65_538
      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)

      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP1.Pool}
      refute_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      refute_receive {[:finch, :connect, :start], _ref, _measurements, _metadata}, 100
    end

    @tag pool_opts: [conn_opts: []]
    test "http pools end up with HTTP/1", %{finch_name: finch_name} do
      bypass = Bypass.open()
      Bypass.expect(bypass, fn conn -> Plug.Conn.send_resp(conn, 200, "OK") end)
      url = "http://localhost:#{bypass.port}"
      pool_name = Finch.Pool.to_name(Finch.Pool.new(url))

      assert {:ok, %{status: 200}} = Finch.build(:get, url) |> Finch.request(finch_name)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP1.Pool}
    end

    test "concurrent first requests share one negotiation", %{
      url: url,
      finch_name: finch_name,
      pool_name: pool_name
    } do
      results =
        1..20
        |> Task.async_stream(fn _ -> Finch.build(:get, url) |> Finch.request(finch_name) end,
          max_concurrency: 20
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, %{status: 200}}, &1))
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      refute_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      refute_receive {[:finch, :connect, :start], _ref, _measurements, _metadata}, 100
    end

    @tag pools: :url
    test "configured pools negotiate on their first request", %{
      url: url,
      finch_name: finch_name,
      pool_name: pool_name
    } do
      assert_received {:register, ^finch_name, ^pool_name, factory, Finch.ALPN.Pool}
      refute_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}

      assert {:ok, %{status: 200}} = Finch.build(:get, url) |> Finch.request(finch_name)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      assert_received {:unregister, ^finch_name, ^pool_name, ^factory}
    end

    @tag pool_opts: [conn_opts: [transport_opts: [verify: :verify_none, timeout: 200]]]
    test "concurrent first requests share one failed negotiation", %{finch_name: finch_name} do
      # Nobody accepts, so the TLS handshake times out
      {:ok, socket} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(socket)
      url = "https://localhost:#{port}"

      results =
        1..20
        |> Task.async_stream(fn _ -> Finch.build(:get, url) |> Finch.request(finch_name) end,
          max_concurrency: 20
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:error, %Finch.TransportError{reason: :timeout}}, &1))
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      refute_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
    end

    test "returns the error when the connection fails", %{
      closed_url: url,
      finch_name: finch_name
    } do
      pool_name = Finch.Pool.to_name(Finch.Pool.new(url))

      assert {:error, %Finch.TransportError{reason: :econnrefused}} =
               Finch.build(:get, url) |> Finch.request(finch_name)

      # The factory stays to negotiate again on the next request
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}
      refute_received {:unregister, ^finch_name, ^pool_name, _pid}
    end

    @tag pool_opts: [count: 2]
    test "shards negotiate on their own", %{
      url: url,
      finch_name: finch_name,
      pool_name: pool_name,
      negotiating: negotiating
    } do
      request = Finch.build(:get, url)

      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      refute_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      refute_receive {[:finch, :connect, :start], _ref, _measurements, _metadata}, 100

      assert {:ok, %{status: 200}} =
               Finch.request(request, finch_name, pool_strategy: negotiating)

      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      refute_receive {[:finch, :connect, :start], _ref, _measurements, _metadata}, 100
    end

    @tag pool_opts: [count: 2]
    test "a shard whose pool keeps crashing is restarted", %{
      url: url,
      finch_name: finch_name,
      pool_name: pool_name,
      negotiating: negotiating
    } do
      request = Finch.build(:get, url)
      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}
      assert_received {:register, ^finch_name, ^pool_name, pool, Finch.HTTP2.Pool}

      # The shard supervisor gives the pool up once its restarts run out, then
      # the shard itself is restarted
      pool =
        Enum.reduce(1..3, pool, fn _, pool ->
          Process.exit(pool, :kill)
          assert_receive {:register, ^finch_name, ^pool_name, pool, Finch.HTTP2.Pool}, 1_000
          pool
        end)

      Process.exit(pool, :kill)
      assert_receive {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}, 1_000
      assert {:ok, 2} = Finch.get_pool_count(finch_name, url)

      assert {:ok, %{status: 200}} =
               Finch.request(request, finch_name, pool_strategy: negotiating)

      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
    end

    @tag pool_opts: [pool_max_idle_time: 100]
    test "an idle HTTP/1 pool stops and its shard negotiates again", %{
      http1_url: url,
      finch_name: finch_name
    } do
      pool_name = Finch.Pool.to_name(Finch.Pool.new(url))
      request = Finch.build(:get, url)

      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}
      assert_received {:register, ^finch_name, ^pool_name, pool, Finch.HTTP1.Pool}

      # The pool stops, the shard is restarted and negotiates again
      assert_receive {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}, 2_000
      refute Process.alive?(pool)

      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP1.Pool}
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
      assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
    end

    @tag pools: :url, pool_opts: [start_pool_metrics?: true, count: 2]
    test "get_pool_status/2 reports the status of the negotiated shards", %{
      url: url,
      finch_name: finch_name,
      negotiating: negotiating
    } do
      request = Finch.build(:get, url)
      assert {:error, :not_found} = Finch.get_pool_status(finch_name, url)

      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
      assert {:ok, [%Finch.HTTP2.PoolMetrics{}]} = Finch.get_pool_status(finch_name, url)

      assert {:ok, %{status: 200}} =
               Finch.request(request, finch_name, pool_strategy: negotiating)

      assert {:ok, [%Finch.HTTP2.PoolMetrics{}, %Finch.HTTP2.PoolMetrics{}] = status} =
               Finch.get_pool_status(finch_name, url)

      assert Enum.map(status, & &1.pool_index) |> Enum.sort() == [1, 2]
    end

    @tag pools: :url
    test "set_pool_count/3 adds shards that negotiate on their own", %{
      url: url,
      finch_name: finch_name,
      pool_name: pool_name,
      negotiating: negotiating
    } do
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}
      assert {:ok, 1} = Finch.get_pool_count(finch_name, url)

      assert :ok = Finch.set_pool_count(finch_name, url, 2)
      assert {:ok, 2} = Finch.get_pool_count(finch_name, url)
      assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}

      request = Finch.build(:get, url)
      assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
      assert_received {:register, ^finch_name, ^pool_name, pool1, Finch.HTTP2.Pool}

      assert {:ok, %{status: 200}} =
               Finch.request(request, finch_name, pool_strategy: negotiating)

      assert_received {:register, ^finch_name, ^pool_name, pool2, Finch.HTTP2.Pool}

      assert :ok = Finch.set_pool_count(finch_name, url, 1)
      assert {:ok, 1} = Finch.get_pool_count(finch_name, url)
      assert Enum.count([pool1, pool2], &Process.alive?/1) == 1
    end
  end

  test "start_pool/3 starts a pool that negotiates on its first request", %{
    url: url,
    finch_name: finch_name
  } do
    pool = Finch.Pool.new(url, tag: :api)
    pool_name = Finch.Pool.to_name(pool)

    assert :ok = Finch.start_pool(finch_name, pool, @pool_opts)
    assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}

    request = Finch.build(:get, url, [], nil, pool_tag: :api)
    assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
    assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
    assert_received {[:finch, :connect, :start], _ref, _measurements, _metadata}
    refute_receive {[:finch, :connect, :start], _ref, _measurements, _metadata}, 100
  end

  test "user-managed pools negotiate on their first request", %{
    url: url,
    finch_name: finch_name
  } do
    pool = Finch.Pool.new(url, tag: :usermanaged)
    pool_name = Finch.Pool.to_name(pool)

    start_supervised!({Finch.Pool, [finch: finch_name, pool: pool] ++ @pool_opts})
    assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.ALPN.Pool}

    request = Finch.build(:get, url, [], nil, pool_tag: :usermanaged)
    assert {:ok, %{status: 200}} = Finch.request(request, finch_name)
    assert_received {:register, ^finch_name, ^pool_name, _pid, Finch.HTTP2.Pool}
  end
end
