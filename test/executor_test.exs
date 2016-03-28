defmodule Quantum.ExecutorTest do
  use ExUnit.Case

  @default_timezone Application.get_env(:quantum, :timezone, :utc)

  import Quantum.Executor

  def ok,     do: :ok
  def ret(v), do: v

  test "check timer aliasing" do
    {last_run, _} = execute({"* * * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
    refute execute({"* * * * *", &ok/0, [], @default_timezone, last_run}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
  end

  test "check minutely" do
    assert execute({"* * * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
  end

  test "check hourly" do
    assert execute({"0 * * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
    refute execute({"0 * * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 1, w: 1})
    assert execute({"@hourly",   &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
    refute execute({"@hourly",   &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 1, w: 1})
  end

  test "check daily" do
    assert execute({"0 0 * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 0, m: 0, w: 1})
    refute execute({"0 0 * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 0, m: 1, w: 1})
    assert execute({"@daily",    &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 0, m: 0, w: 1})
    refute execute({"@daily",    &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 0, m: 1, w: 1})
    assert execute({"@midnight", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 0, m: 0, w: 1})
    refute execute({"@midnight", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 0, m: 1, w: 1})
  end

  test "check weekly" do
    assert execute({"0 0 * * 0", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 27}, h: 0, m: 0, w: 0})
    refute execute({"0 0 * * 0", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 27}, h: 0, m: 1, w: 0})
    assert execute({"@weekly",   &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 27}, h: 0, m: 0, w: 0})
    refute execute({"@weekly",   &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 27}, h: 0, m: 1, w: 0})
  end

  test "check monthly" do
    assert execute({"0 0 1 * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 1}, h: 0, m: 0, w: 0})
    refute execute({"0 0 1 * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 1}, h: 0, m: 1, w: 0})
    assert execute({"@monthly",  &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 1}, h: 0, m: 0, w: 0})
    refute execute({"@monthly",  &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 1}, h: 0, m: 1, w: 0})
  end

  test "check yearly" do
    assert execute({"0 0 1 1 *", &ok/0, [], @default_timezone, :never}, %{d: {2016, 1, 1}, h: 0, m: 0, w: 0})
    refute execute({"0 0 1 1 *", &ok/0, [], @default_timezone, :never}, %{d: {2016, 1, 1}, h: 0, m: 1, w: 0})
    assert execute({"@annually", &ok/0, [], @default_timezone, :never}, %{d: {2016, 1, 1}, h: 0, m: 0, w: 0})
    refute execute({"@annually", &ok/0, [], @default_timezone, :never}, %{d: {2016, 1, 1}, h: 0, m: 1, w: 0})
    assert execute({"@yearly",   &ok/0, [], @default_timezone, :never}, %{d: {2016, 1, 1}, h: 0, m: 0, w: 0})
    refute execute({"@yearly",   &ok/0, [], @default_timezone, :never}, %{d: {2016, 1, 1}, h: 0, m: 1, w: 0})
  end

  test "parse */5" do
    assert execute({"*/5 * * * *", &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
  end

  test "parse 5" do
    assert execute({"5 * * * *",  &ok/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 5, w: 1})
  end

  test "counter example" do
    refute execute({"5 * * * *", &flunk/0, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
  end

  test "function as tuple" do
    assert execute({"* * * * *", {__MODULE__, :ok}, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
    assert execute({"* * * * *", {"Quantum.ExecutorTest", "ok"}, [], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
  end

  test "readable schedule" do
    assert execute({"@weekly", {__MODULE__, :ok}, [], @default_timezone, :never}, %{d: {2015, 12, 27}, h: 0, m: 0, w: 0})
  end

  test "function with args" do
    assert execute({"* * * * *", &ret/1, [:passed], @default_timezone, :never}, %{d: {2015, 12, 31}, h: 12, m: 0, w: 1})
  end

  test "reboot" do
    assert execute({"@reboot", &ok/0, [], @default_timezone, :never}, %{r: 1})
    refute execute({"@reboot", &ok/0, [], @default_timezone, :never}, %{r: 0})
  end

end
