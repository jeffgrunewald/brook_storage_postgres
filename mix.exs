defmodule BrookStoragePostgres.MixProject do
  use Mix.Project

  def project do
    [
      app: :brook_storage_postgres,
      version: "0.1.0",
      elixir: "~> 1.8",
      description: description(),
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:brook, "~> 0.4"},
      {:brook_serializer, "~> 2.0"},
      {:divo, "~> 1.1", only: [:dev, :integration]},
      {:divo_postgres, "~> 0.1", only: :integration},
      {:postgrex, "~> 0.15"}
    ]
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]

  defp package do
    [
      maintainers: ["Jeff Grunewald"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/jeffgrunewald/brook_storage_postgres"}
    ]
  end

  defp description do
    "An implementation of the Brook event stream storage behaviour
    for the Postgres database."
  end
end
