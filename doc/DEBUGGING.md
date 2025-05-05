# Using a Debugger with SurrealDB

To use the VSCode debugger with SurrealDB there are a few steps to get setup. This guide walks you
through setup and configuration.

## Setup

Install [CodeLLDB](https://marketplace.visualstudio.com/items/?itemName=vadimcn.vscode-lldb) 1.11.5
or greater.

Settings:

* Set [`LLDB → Launch: Breakpoint Mode`](vscode://settings/lldb.launch.breakpointMode) to `path`.
* Set [`LLDB → Launch: Terminal`](vscode://settings/lldb.launch.terminal) to `integrated`.
* Set [`LLDB → Launch: Init Commands`](vscode://settings/lldb.launch.initCommands) to:

    ```json
    "lldb.launch.initCommands": [
       "command script import ${userHome}/.rustup/toolchains/1.86-aarch64-apple-darwin/lib/rustlib/etc/lldb_lookup.py",
       "command source ${userHome}/.rustup/toolchains/1.86-aarch64-apple-darwin/lib/rustlib/etc/lldb_commands"
    ]
    ```

## Launch Configuration

The `launch.json` is used to specify debugger configuration. The following configuration will setup
the interactive `sql` shell to run in the debugger so you can run adhoc commands and inspect the
state of the program.

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "lldb",
            "request": "launch",
            "name": "Debug surrealdb",
            "cargo": {
                "args": [
                    "build",
                    "--no-default-features",
                    "--features",
                    "storage-mem,http,scripting"
                ],
            },
            "args": [
                "sql",
                "--ns", "ns",
                "--db", "db",
                "--endpoint", "memory"
            ],
            "cwd": "${workspaceFolder}",
            "sourceLanguages": ["rust"]
        }
    ]
}
```

## Running the Debugger

1. Add breakpoints in the code you want to debug. You can do this by clicking in the gutter next to
   the line number.
2. Open the `Debug` view in VSCode. (`Ctrl+Shift+D` or `Cmd+Shift+D` on macOS).
3. Select the `Debug surrealdb` configuration. (`F5`)
4. The debugger will start and the `sql` shell will be launched.
5. You can now run SQL commands in the shell and the debugger will stop at the breakpoints you set.
6. Use the debugger controls to step through the code, inspect variables, and evaluate expressions.
7. You can also use the `Debug Console` to run commands and inspect the state of the program.
8. When you are done debugging, you can stop the debugger by clicking the `Stop` button in the
   `Debug` view or by pressing `Shift+F5`.
