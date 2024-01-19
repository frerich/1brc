defmodule M do
  def slurp_and_split(filename) do
    filename
    |> File.read!()
    |> String.split("\n")
  end

  def slurp_and_recurse(filename) do
    filename
    |> File.read!()
    |> parse_lines_2({[], []})
  end

  defp parse_lines(<<>>, {buf, lines}) do
    Enum.reverse([buf | lines])
  end

  defp parse_lines(<<?\n, rest::binary>>, {buf, lines}) do
    parse_lines(rest, {[], [to_string(Enum.reverse(buf)) | lines]})
  end

  defp parse_lines(<<char, rest::binary>>, {buf, lines}) do
    parse_lines(rest, {[char | buf], lines})
  end

  defp parse_lines_2(data, {[], lines}) do
    case String.split(data, "\n", parts: 2) do
      [line, rest] -> parse_lines_2(rest, {[], [line | lines]})
      [line] -> Enum.reverse([line | lines])
    end
  end

  def via_io_device(filename) do
    data = File.read!(filename)

    {:ok, pid} = StringIO.open(data)

    Stream.repeatedly(fn -> IO.binread(pid, :line) end)
    |> Enum.take_while(& &1 != :eof)
  end

  def via_file_readline_stream(filename) do
    File.open!(filename, [:read, :raw, {:read_ahead, 65536}], fn file ->
      Stream.repeatedly(fn -> :file.read_line(file) end)
      |> Enum.take_while(fn
        {:ok, _line} -> true
        _ -> false
      end)
    end)
  end

  def via_file_readline_stream_using_match(filename) do
    File.open!(filename, [:read, :raw, {:read_ahead, 65536}], fn file ->
      Stream.repeatedly(fn -> :file.read_line(file) end)
      |> Enum.take_while(& match?({:ok, _line}, &1))
    end)
  end

  def via_file_readline_manual_recursion(filename) do
    File.open!(filename, [:read, :raw, {:read_ahead, 65536}], fn file ->
      count_lines_2(file)
    end)
  end

  defp count_lines_2(file, acc \\ []) do
    case :file.read_line(file) do
      {:ok, line} ->
        count_lines_2(file, [line | acc])

      _ ->
        Enum.reverse(acc)
    end
  end
end

filename = "data/measurements_10M.txt"
:timer.tc(fn -> M.slurp_and_split("data/measurements_1B.txt") |> Enum.count() end) |> dbg()
:timer.tc(fn -> M.slurp_and_recurse(filename) |> Enum.count() end) |> dbg()
:timer.tc(fn -> M.via_io_device(filename) |> Enum.count() end) |> dbg()
:timer.tc(fn -> M.via_file_readline_stream(filename) |> Enum.count() end) |> dbg()
:timer.tc(fn -> M.via_file_readline_stream_using_match(filename) |> Enum.count() end) |> dbg()
:timer.tc(fn -> M.via_file_readline_manual_recursion(filename) |> Enum.count() end) |> dbg()
