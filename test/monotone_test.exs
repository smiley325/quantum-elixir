defmodule Quantum.MonotoneTest do
  use ExUnit.Case

  @ms_in_seconds Application.get_env(:quantum, :ms_in_seconds, 1000)

  setup do
    Quantum.delete_all_jobs

    if :ets.info(:counter) == :undefined do
        :ets.new(:counter, [:set, :public, :named_table])
        :ets.insert(:counter, {:count, 0})
    end

    :ok
  end

  test "test job monotone" do
    spec = "* * * * *"
    fun = fn -> 
        :ets.update_counter(:counter, :count, 1)
    end

    :ok = Quantum.add_job(spec, fun)
    job = %Quantum.Job{schedule: spec, task: fun, nodes: [node()]}

    :timer.sleep(10 * 60 * @ms_in_seconds)

    ec = :ets.lookup(:counter, :count)[:count]
    
    assert ec >= 1
    assert ec <= 2
  end
end