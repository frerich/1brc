defmodule M do
  def process(path, batch_size) do
    path
    |> File.stream!(65536)
    |> binary_stream_to_lines()
    |> Stream.chunk_every(batch_size)
    |> Task.async_stream(fn lines ->
      for line <- lines do
        {station, measurement} = parse_line(line)

        key = "station_#{station}"

        {min, {sum, count}, max} = Process.get(key, {1000, {0, 0}, -1000})

        Process.put(key, {
          min(min, measurement),
          {sum + measurement, count + 1},
          max(max, measurement)
        })
      end

      for <<"station_", station::binary>> = key <- Process.get_keys(), into: %{} do
      {station, Process.get(key)}
      end
    end)
    |> Enum.reduce(%{}, fn {:ok, stats}, acc ->
      Map.merge(acc, stats, fn _station, {min_a, {sum_a, count_a}, max_a}, {min_b, {sum_b, count_b}, max_b} ->
        {min(min_a, min_b), {sum_a + sum_b, count_a + count_b}, max(max_a, max_b)}
      end)
    end)
    |> Enum.sort_by(fn {station, _data} -> station end)
    |> Enum.map_join("\n", fn {station, {min, {sum, count}, max}} ->
      "#{station}=#{min}/#{Float.round(sum / count, 1)}/#{max}"
    end)
    |> IO.puts()
  end

  def binary_stream_to_lines(stream) do
    Stream.transform(stream, "", fn binary, acc ->
      [rest | lines] = Enum.reverse(String.split(acc <> binary, "\n"))
      {Enum.reverse(lines), rest}
    end)
  end

  def parse_line(line) when is_binary(line) do
    [station, temp] = String.split(line, ";", parts: 2)
    {temp, ""} = Float.parse(temp)
    {station, temp}
  end
end

path = "data/measurements_10M.txt"
{runtime_ms, _} = :timer.tc(M, :process, [path, 1_000_000], :millisecond)
IO.puts("#{runtime_ms}ms")
