defmodule BrookStoragePostgres.MixProject do
  use Mix.Project

  @github "https://github.com/jeffgrunewald/brook_storage_postgres"

  def project do
    [
      app: :brook_storage_postgres,
      version: "0.1.3",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      description: description(),
      package: package(),
      source_url: @github,
      homepage_url: @github,
      dialyzer: [plt_file: {:no_warn, ".dialyzer/#{System.version()}.plt"}]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:brook, "~> 0.6"},
      {:brook_serializer, "~> 2.2"},
      {:postgrex, "~> 0.15"},
      {:ex_doc, "~> 0.24"},
      {:dialyxir, "~> 1.1", only: [:dev], runtime: false},
      {:divo_postgres, "~> 0.2", only: [:test]},
      {:placebo, "~> 2.0", only: [:test]}
    ]
  end

  defp package do
    [
      maintainers: ["Jeff Grunewald"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => "https://github.com/jeffgrunewald/brook_storage_postgres"}
    ]
  end

  defp docs do
    [
      source_url: @github,
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp description do
    "An implementation of the Brook event stream storage behaviour
    for the Postgres database."
  end
end
