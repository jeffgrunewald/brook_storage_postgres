use Mix.Config

config :brook_storage_postgres,
  divo: [
    {DivoPostgres, [database: "view_test", user: "brook", password: "brook"]}
  ],
  divo_wait: [dwell: 500, max_tries: 25]
