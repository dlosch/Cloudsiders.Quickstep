## Readme

Simple adapter for Orleans streaming, based on SQS adapter.

[**Microsoft Orleans**](https://github.com/dotnet/orleans) stream provider using [**Microsoft.FASTER**](https://github.com/microsoft/FASTER) log implementation

### Work in progress (_Finger&uuml;bung_)

- uses a single local FASTER log file
- serialization using standard Orleans mechanism, albeit with code borrowed from Orleans repo

### Configuration

>
> "FasterLogAdapterOptions": {
>         "LogFile": "f:\\temp\\orleans.log",
>         "LogSegmentSizeBits": 26,
>         "UsePersistentLog": true,
>         "PreallocateLogFile": true,
>         "CommitPeriodMillis": 1000,
v         "UseWatermarkForCommits": true
>       }
>