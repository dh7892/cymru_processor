# cymru_processor

To run this you need to have the general rust toolchain set up.

will need to set the following env vars before runnings:

DB_HOST
DB_USER
DB_PASS

You can build and execute the program with this command:

`cargo run --release`

This will expect to find a file called `data.xml.gz`

It will unzip this on the fly and extract the data, inserting them in batches of 1000 into a table
called `sink.dave_team_cymru_repfeed`

It will show a progress bar with an expected time remaining so you can track progress.
