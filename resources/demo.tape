# brew install vhs
# vhs demo.tape

Output demo.gif

Set FontSize 16
Set Width 1250
Set Height 640
Set LineHeight 1.5
Set TypingSpeed 0.01
Set Framerate 60

Type "omniload ingest --source-uri $POSTGRESQL_URI --source-table 'information_schema.tables' --dest-uri $DUCKDB_URI --dest-table 'pg.tables'"
Enter

Sleep 2s

Type "y"
Enter

Sleep 12s
