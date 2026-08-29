defmodule Finch.ALPN.Pool do
  @moduledoc false
  # The pool for `protocols: [:http1, :http2]`. Each of its shards is a
  # supervisor of a `Finch.ALPN.Factory`, which connects on the shard's first
  # request and makes the HTTP1 or HTTP2 pool for the protocol Mint negotiated
  # next to itself. From then on requests go straight to that pool.

  # The shard is permanent like an HTTP2 shard: the pool inside comes and goes
  # (an idle HTTP1 pool stops, a pool that keeps crashing is given up on) and
  # the shard negotiates again on its next request
  use Supervisor
  @behaviour Finch.Pool.Manager

  alias Finch.ALPN.Factory

  def start_link({_pool, _pool_name, _registry_name, _pool_config, _pool_idx} = arg) do
    Supervisor.start_link(__MODULE__, arg)
  end

  # The shard shuts down when the factory crashes or when the pool stops, see
  # Finch.Pool.Supervisor.build_child_spec/4, and is restarted by its supervisor
  @impl Supervisor
  def init(arg) do
    children = [{Factory, {self(), arg}}]
    Supervisor.init(children, auto_shutdown: :any_significant, strategy: :one_for_one)
  end

  @impl Finch.Pool.Manager
  def request(factory, req, acc, fun, name, opts) do
    case Factory.pool(factory) do
      {:ok, {pool, mod}} -> mod.request(pool, req, acc, fun, name, opts)
      {:error, error} -> {:error, error, acc}
    end
  end

  @impl Finch.Pool.Manager
  def async_request(factory, req, name, opts) do
    case Factory.pool(factory) do
      {:ok, {pool, mod}} ->
        mod.async_request(pool, req, name, opts)

      {:error, error} ->
        request_ref = {__MODULE__, make_ref()}
        send(self(), {request_ref, {:error, error}})
        request_ref
    end
  end

  @impl Finch.Pool.Manager
  def cancel_async_request({__MODULE__, _ref}), do: :ok

  # The shards connect on their own, so they do not necessarily use the same protocol
  @impl Finch.Pool.Manager
  def get_pool_status(finch_name, pool_name) do
    case collect_pool_status(finch_name, pool_name) do
      [] -> {:error, :not_found}
      status -> {:ok, status}
    end
  end

  defp collect_pool_status(finch_name, pool_name) do
    finch_name
    |> Factory.started(pool_name)
    |> Enum.flat_map(fn {mod, pool_idxs} ->
      case mod.get_pool_status(finch_name, pool_name) do
        {:ok, status} -> Enum.filter(status, &(&1.pool_index in pool_idxs))
        {:error, :not_found} -> []
      end
    end)
  end
end
