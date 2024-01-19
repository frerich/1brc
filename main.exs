Mix.install([:benchee])

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

  # Generate parsing functions for all required patterns
  for city_length <- 1..30, whole_part_length <- 1..2 do
    defp parse_line(<<city::binary-size(unquote(city_length)), ?;, ?-, whole_part::binary-size(unquote(whole_part_length)), ?., fractional_part::binary-1>>) do
      {city, to_integer(whole_part) * -1 + to_integer(fractional_part) / 10}
    end

    defp parse_line(<<city::binary-size(unquote(city_length)), ?;, whole_part::binary-size(unquote(whole_part_length)), ?., fractional_part::binary-1>>) do
      {city, to_integer(whole_part) + to_integer(fractional_part) / 10}
    end
  end

  # Optimised string -> integer conversions for all required values
  for i <- 0..99 do
    defp to_integer(unquote(to_string(i))), do: unquote(i)
  end
end

path = "data/measurements_10M.txt"

{runtime_usecs, _} = :timer.tc(fn -> M.process(path, 1_000_000) end)
IO.puts("Runtime: #{runtime_usecs}us")

#:fprof.apply(M, :process, [path, 10_000])
#:fprof.profile()
#:fprof.analyse(:dest, 'outfile.fprof')

#Benchee.run(%{
#  "parse via String.split/3" => fn -> M.parse_line("Hamburg;13.4") end,
#  "parse via Regex.run/2" => fn -> M.parse_line_via_regex("Hamburg;13.4") end,
#  "parse via pattern matching" => fn -> M.parse_line_via_pattern_match("Hamburg;13.4") end
#  })
