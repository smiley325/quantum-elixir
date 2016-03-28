defmodule Quantum.Executor do

  @moduledoc false

  alias Timex.Timezone
  alias Timex.DateTime
  import Quantum.Matcher

  def convert_to_timezone(s, tz) do
    t = {s.d, {s.h, s.m, 0}}  # Convert to erlang datetime
    tz_final = case tz do
      :utc   -> Timezone.get("UTC")
      :local -> Timezone.local()
      tz0    -> tz0
    end

    case Application.get_env(:quantum, :timezone, :utc) do
      :utc   -> t |> DateTime.from |> Timezone.convert(tz_final)
      :local -> t |> DateTime.from(:local) |> Timezone.convert(tz_final)
      tz     -> raise "Unsupported timezone: #{tz}"
    end
  end

  @doc ~S"""
  Convert Timex.DateTime to {{y, m, d}, hr, min} format.
  """
  def to_std_time(dt) do
    {{dt.year, dt.month, dt.day}, dt.hour, dt.minute}
  end

  @doc ~S"""
  Check that the job has not already run in this time quantum.
  """
  def already_run(:never, current_time) do
    false
  end

  def already_run({{y, m, d}, hr, min}, c) do
    c.month == m and c.day == d and c.hour == hr and c.minute == min
  end

  def execute({"@reboot",   fun, args, tz, lr}, %{r: 1}), do: execute_fun(fun, args)
  def execute(_,                        %{r: 1}), do: false
  def execute({"@reboot",   _, _, _, _},   %{r: 0}), do: false

  def execute({"@hourly", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    if not already_run(lr, c) and c.minute == 0 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({"@daily", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    if not already_run(lr, c) and c.minute == 0 and c.hour == 0 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({"@midnight", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    if not already_run(lr, c) and c.minute == 0 and c.hour == 0 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({"@weekly", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    c_weekday = rem(Timex.weekday(c), 7)
    if not already_run(lr, c) and c.minute == 0 and c.hour == 0 and c_weekday == 0 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({"@monthly", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    if not already_run(lr, c) and c.minute == 0 and c.hour == 0 and c.day == 1 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({"@annually", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    if not already_run(lr, c) and c.minute == 0 and c.hour == 0 and c.day == 1 and c.month == 1 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({"@yearly", fun, args, tz, lr}, state) do
    c = convert_to_timezone(state, tz)
    if not already_run(lr, c) and c.minute == 0 and c.hour == 0 and c.day == 1 and c.month == 1 do
      {to_std_time(c), execute_fun(fun, args)}
    else
      false
    end
  end

  def execute({e, fun, args, tz, lr}, state) do
    [m, h, d, n, w] = e |> String.split(" ")

    c = convert_to_timezone(state, tz)
    c_weekday = rem(Timex.weekday(c), 7)

    cond do
      already_run(lr, c)          -> false
      !match(m, c.minute,  0..59) -> false
      !match(h, c.hour,    0..23) -> false
      !match(d, c.day,     1..31) -> false
      !match(n, c.month,   1..12) -> false
      !match(w, c_weekday, 0..6)  -> false
      true                        -> {to_std_time(c), execute_fun(fun, args)}
    end
  rescue
    e -> false
  end

  defp execute_fun({mod, fun}, args) do
    Task.Supervisor.async_nolink(:quantum_tasks_sup, fn ->
      mod = if is_binary(mod), do: String.to_atom("Elixir.#{mod}"), else: mod
      fun = if is_binary(fun), do: String.to_atom(fun), else: fun
      :erlang.apply(mod, fun, args)
    end)
  end

  defp execute_fun(fun, args) do
    Task.Supervisor.async_nolink(:quantum_tasks_sup, fn ->
      :erlang.apply(fun, args)
    end)
  end
end
