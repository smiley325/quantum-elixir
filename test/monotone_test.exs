defmodule Quantum.MonotoneTest do
  use ExUnit.Case

  setup do
    Quantum.delete_all_jobs

    if :ets.info(:counter) == :undefined do
        :ets.new(:counter, [:set, :public, :named_table])
        :ets.insert(:counter, {:count, 0})
    end
  end

  test "test job monotone" do
    spec = "* * * * *"
    fun = fn -> 
        :ets.update_counter(:counter, :count, 1)
    end

    :ok = Quantum.add_job(spec, fun)
    job = %Quantum.Job{schedule: spec, task: fun, nodes: [node()]}

    :timer.sleep(10 * 60 * 1000)

    ec = :ets.lookup(:counter, :count)[:count]
    assert ec == 10
  end
end