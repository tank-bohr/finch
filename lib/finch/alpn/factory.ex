defmodule Finch.ALPN.Factory do
  @moduledoc false
  # Makes the pool of a shard of a `Finch.ALPN.Pool`, and stands in for it in
  # the registry until then: on the first request it connects to the server,
  # starts the HTTP1 or HTTP2 pool for the protocol Mint negotiated under the
  # shard supervisor, hands it the connection and unregisters itself.

  @behaviour :gen_statem

  alias Finch.Error
  alias Finch.Pool.Manager
  alias Finch.SSL
  alias Finch.Telemetry

  # The shard shuts down with the factory, see Finch.ALPN.Pool
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]},
      restart: :temporary,
      significant: true
    }
  end

  def start_link({_supervisor, {_pool, pool_name, registry_name, _pool_config, pool_idx}} = arg) do
    :gen_statem.start_link(name(registry_name, pool_name, pool_idx), __MODULE__, arg, [])
  end

  # Registered in the supervisor registry with the module of the started pool
  # as the value, so the status of the shards can be told apart by protocol
  defp name(registry_name, pool_name, pool_idx) do
    {:via, Registry, {Manager.supervisor_registry_name(registry_name), key(pool_name, pool_idx)}}
  end

  defp key(pool_name, pool_idx), do: {__MODULE__, pool_name, pool_idx}

  @doc false
  # The pool of the shard, made on the first call
  @spec pool(pid()) :: {:ok, {pid(), module()}} | {:error, Exception.t()}
  def pool(factory), do: :gen_statem.call(factory, :pool)

  @doc false
  # The shards whose pool is started, by pool module
  @spec started(Finch.name(), Manager.pool_name()) :: %{module() => [pos_integer()]}
  def started(registry_name, pool_name) do
    spec = [{{key(pool_name, :"$1"), :_, :"$2"}, [{:"=/=", :"$2", nil}], [{{:"$2", :"$1"}}]}]

    registry_name
    |> Manager.supervisor_registry_name()
    |> Registry.select(spec)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
  end

  @doc false
  # Shards of an ALPN pool ask for the connection the protocol was negotiated
  # on when they start. There is none once the pool is started, which the
  # factory records in its registry value
  @spec request_seed(Finch.name(), Finch.Pool.t(), pos_integer()) :: :ok | :none
  def request_seed(registry_name, %Finch.Pool{} = pool, pool_idx) do
    key = key(Finch.Pool.to_name(pool), pool_idx)

    case Registry.lookup(Manager.supervisor_registry_name(registry_name), key) do
      [{pid, nil}] -> :gen_statem.cast(pid, {:seed?, self()})
      _ -> :none
    end
  end

  @impl :gen_statem
  def callback_mode(), do: :state_functions

  @impl :gen_statem
  def init({supervisor, {pool, pool_name, registry_name, pool_config, pool_idx}}) do
    {:ok, _} = Registry.register(registry_name, pool_name, Finch.ALPN.Pool)

    data = %{
      supervisor: supervisor,
      pool: pool,
      pool_name: pool_name,
      registry_name: registry_name,
      pool_config: pool_config,
      pool_idx: pool_idx,
      task: nil,
      conn: nil,
      waiting: [],
      entry: nil
    }

    {:ok, :idle, data}
  end

  # The callers wait for the outcome of the connection, made in a task so that
  # the factory keeps taking them in
  def idle({:call, from}, :pool, data) do
    factory = self()
    task = Task.async(fn -> connect(data, factory) end)
    {:next_state, :connecting, %{data | task: task, waiting: [from]}}
  end

  def connecting({:call, from}, :pool, data) do
    {:keep_state, %{data | waiting: [from | data.waiting]}}
  end

  def connecting(:info, {ref, result}, %{task: %Task{ref: ref}} = data) do
    Process.demonitor(ref, [:flush])
    data = %{data | task: nil}

    case start_pool(data, result) do
      {:ok, data} ->
        {:next_state, :starting, data}

      {:error, error} ->
        {:next_state, :idle, %{data | waiting: []}, replies(data, {:error, error})}
    end
  end

  # The pool takes the connection as it starts, then the callers are answered
  def starting({:call, from}, :pool, data) do
    {:keep_state, %{data | waiting: [from | data.waiting]}}
  end

  def starting(:cast, {:seed?, pool}, %{conn: conn, pool_config: %{mod: mod}} = data) do
    %{registry_name: registry_name, pool_name: pool_name, pool_idx: pool_idx} = data
    :ok = mod.seed(pool, conn)
    Registry.unregister(registry_name, pool_name)
    registry = Manager.supervisor_registry_name(registry_name)
    {^mod, nil} = Registry.update_value(registry, key(pool_name, pool_idx), fn _ -> mod end)
    entry = {pool, mod}
    replies = replies(data, {:ok, entry})
    {:next_state, :started, %{data | conn: nil, waiting: [], entry: entry}, replies}
  end

  def started({:call, from}, :pool, %{entry: entry}) do
    {:keep_state_and_data, {:reply, from, {:ok, entry}}}
  end

  # The pool was restarted before the factory recorded it, it connects on its own
  def started(:cast, {:seed?, pool}, %{entry: {_pool, mod}} = data) do
    {:keep_state, %{data | entry: {pool, mod}}}
  end

  defp replies(%{waiting: waiting}, reply), do: Enum.map(waiting, &{:reply, &1, reply})

  defp start_pool(data, {:ok, conn}) do
    %{supervisor: supervisor, pool: pool, registry_name: registry_name, pool_idx: pool_idx} = data
    pool_config = pool_config(data.pool_config, Mint.HTTP.protocol(conn))
    spec = Finch.Pool.Supervisor.build_child_spec(pool, registry_name, pool_config, pool_idx)

    case Supervisor.start_child(supervisor, spec) do
      {:ok, _pid} ->
        {:ok, %{data | pool_config: pool_config, conn: conn}}

      {:error, reason} ->
        Mint.HTTP.close(conn)
        {:error, Error.wrap(reason)}
    end
  end

  defp start_pool(_data, {:error, error}), do: {:error, error}

  # The connection is handed over to the factory, which hands it over to the pool
  defp connect(%{pool: pool, pool_config: pool_config, registry_name: finch_name}, factory) do
    conn_opts = Keyword.put(pool_config.conn_opts, :mode, :passive)
    metadata = %{scheme: pool.scheme, host: pool.host, port: pool.port, name: finch_name}
    start_time = Telemetry.start(:connect, metadata)

    case connect(pool, conn_opts, factory) do
      {:ok, conn} ->
        Telemetry.stop(:connect, start_time, metadata)
        SSL.maybe_log_secrets(pool.scheme, conn_opts, conn)
        {:ok, conn}

      {:error, error} ->
        error = Error.wrap(error)
        Telemetry.stop(:connect, start_time, Map.put(metadata, :error, error))
        {:error, error}
    end
  end

  defp connect(pool, conn_opts, factory) do
    with {:ok, conn} <- Mint.HTTP.connect(pool.scheme, pool.host, pool.port, conn_opts) do
      Mint.HTTP.controlling_process(conn, factory)
    end
  end

  defp pool_config(pool_config, protocol) do
    %{
      pool_config
      | mod: pool_mod(protocol),
        conn_opts: Keyword.put(pool_config.conn_opts, :protocols, [protocol])
    }
  end

  defp pool_mod(:http1), do: Finch.HTTP1.Pool
  defp pool_mod(:http2), do: Finch.HTTP2.Pool
end
