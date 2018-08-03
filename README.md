# ioreplay

*ioreplay* is an application used to replay recorded activities performed using [Oneclient](https://github.com/onedata/oneclient)

# Example Usage:

## Disclaimer
IOReplay application has capability to recreate recorded env, but existence of every spaces used in tests is assumed and required.

Typical scenario:
* `ioreplay -s <trace_file>` - will sort trace file. Sort is mandatory just once (will overwrite source file).
* `ioreplay -c -m <mount_path> <trace_file>` - will recreate environment (files and directories but not spaces)
* `ioreplay -r -m <mount_path> <trace_file>` - will perform recorded system calls

where:
* `<trace_file>` - path to trace file left by Oneclient
* `<mount_path>` - path to mounted Oneclient