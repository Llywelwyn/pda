<p align="center"></p><!-- spacer -->

<div align="center">
  <img src="https://raw.githubusercontent.com/llywelwyn/pda/master/docs/pda.png"
       alt="pda"
       width="200" />
</div>

<p align="center"></p><!-- spacer -->

<div align="center">
  <a href="https://github.com/llywelwyn/pda/actions" rel="nofollow">
    <img src="https://img.shields.io/github/actions/workflow/status/llywelwyn/pda/go.yml?branch=main"
         alt="build status"
         style="max-width:100%;">
  </a>
</div>

<p align="center"></p><!-- spacer -->

### Contents

- [Overview](https://github.com/Llywelwyn/pda#overview)
- [Installation](https://github.com/Llywelwyn/pda#installation)
- [Get Started](https://github.com/Llywelwyn/pda#get-started)
- [Templates](https://github.com/Llywelwyn/pda#templates)
- [Secrets](https://github.com/Llywelwyn/pda#secrets)
- [TTL](https://github.com/Llywelwyn/pda#ttl)
- [Binary](https://github.com/Llywelwyn/pda#binary)
- [Environment](https://github.com/Llywelwyn/pda#environment)

<p align="center"></p><!-- spacer -->

### Overview

  ```bash
Available Commands:
    get         # Get a value.
    set         # Set a value.
    del         # Delete a value.
    del-db      # Delete a whole database.
    list-dbs    # List all databases.
    dump        # Export a database as NDJSON.
    restore     # Imports NDJSON into a database.
    completion  # Generate autocompletions for a specified shell.
    help        # Additional help for any command.
```

<p align="center"></p><!-- spacer -->

### Installation

```bash
# Get the latest release from the AUR
yay -S pda

# Or use pda-git for the latest commit
yay -S pda-git

# Or manually install with Go
git clone https://github.com/llywelwyn/pda
cd pda
go install
```

<p align="center"></p><!-- spacer -->

### Get Started

`pda set` to save a key.
```bash
# From arguments
pda set name "Alice"

# From stdin
echo "Alice" | pda set name
cat dogs.txt | pda set dogs
pda set kitty < cat.png
```

<p align="center"></p><!-- spacer -->

`pda get` to retrieve it.
```bash
pda get name
# Alice

# Or run it directly.
pda get name --run
```

<p align="center"></p><!-- spacer -->

`pda del` to delete a key.
```bash
pda del kitty
# Are you sure you want to delete kitty? [y/N]
# y

# Or skip the prompt.
pda del kitty --force
```

<p align="center"></p><!-- spacer -->

`pda ls` to see what you've got stored.
```bash
pda ls
# name    Alice
# dogs    four legged mammals

# Or as CSV.
pda ls --format csv
# name,Alice
# dogs,four legged mammals

# Or TSV, or Markdown, or HTML.
```

<p align="center"></p><!-- spacer -->

`pda dump` to export everything as NDJSON.
```bash
pda dump > my_backup
```

<p align="center"></p><!-- spacer -->

`pda restore` to import it all back.
```bash
# Restore with an argument.
pda restore -f my_backup
# Restored 2 entries into @default.

# Or from stdin.
pda restore < my_backup
# Restored 2 entries into @default.
```

<p align="center"></p><!-- spacer -->

You can have as many stores as you want.
```bash
# Save to a spceific store.
pda set alice@birthdays 11/11/1998

# See which stores have contents.
pda list-dbs
# @default
# @birthdays

# Check out a specific store.
pda ls @birthdays
# alice    11/11/1998
# bob      05/12/1980

# Dump it.
pda dump birthdays > friends_birthdays

# Restore it.
pda restore birthays < friends_birthdays

# Delete it.
pda del-db birthdays --force
```

<p align="center"></p><!-- spacer -->

### Templates

Values support effectively all of Go's `text/template` syntax. Templates are evaluated on `pda get`.

`text/template` is a Turing-complete templating library that supports most of what you'd expect in a scripting language. Actions are given with ``{{ action }}` syntax and support pipelines and nested templates, along with a lot more. I recommend reading the documentation if you want to do anything more complicated than described here.

To fit `text/template` nicely into this tool, pda has a sparse set of additional functions built-in. For example, `default` values, `enum`s, `require`d values, `lists`, among others.

Below is more detail on the extra functions added by this tool.

<p align="center"></p><!-- spacer -->

`{{ .BASIC }}` substitution
```bash
pda set greeting "Hello, {{ .NAME }}"
pda get greeting NAME="Alice"
# Hello, Alice
```

<p align="center"></p><!-- spacer -->

`default` sets a default value.
```bash
pda set greeting "Hello, {{ default "World" .NAME }}"
pda get greeting
# Hello, World
pda get greeting NAME="Bob"
# Hello, Bob
```

<p align="center"></p><!-- spacer -->

`require` errors if missing.
```bash
pda set file "{{ require .FILE }}"
pda get file
# Error: required value missing or empty
```

<p align="center"></p><!-- spacer -->

`env` reads from environment variables.
```bash
pda set my_name "{{ env "USER" }}"
pda get my_name
# llywelwyn
```

<p align="center"></p><!-- spacer -->

`enum` restricts acceptable values.
```bash
pda set level "Log level: {{ enum .LEVEL "info" "warn" "error" }}"
pda get level LEVEL=info
# Log level: info
pda get level LEVEL=debug
# Error: invalid value "debug" (allowed: [info warn error])
```

<p align="center"></p><!-- spacer -->

`int` to parse as an integer.
```bash
pda set number "{{ int .N }}"
pda get number N=3
# 3

# Use it in a loop.
pda set meows "{{ range int .COUNT }}meow! {{ end }}"
pda get meows COUNT=4
# meow! meow! meow! meow!
```

<p align="center"></p><!-- spacer -->

`list` to parse CSV as a list.
```bash
pda set names "{{ range list .NAMES }}Hi {{.}}. {{ end }}"
pda get names NAMES=Bob,Alice
# Hi Bob. Hi Alice.
```

<p align="center"></p><!-- spacer -->

pass `no-template` to output literally without templating.
```bash
pda set hello "{{ if .MORNING }}Good morning.{{ end }}
pda get hello MORNING=1
# Good morning.
pda get hello --no-template
# {{ if .MORNING }}Good morning.{{ end }}
```

<p align="center"></p><!-- spacer -->

### Secrets

Mark sensitive values with `secret` to stop accidents.
```bash
# Store a secret
pda set password "hunter2" --secret
```

<p align="center"></p><!-- spacer -->

`secret` is used for revealing secrets too.
```bash
pda get password
# Error: "password" is marked secret; re-run with --secret to display it
pda get password --secret
# hunter2
```

<p align="center"></p><!-- spacer -->

`list` censors secrets.
```bash
pda ls
# password    ************

pda ls --secret
# password    hunter2
```

<p align="center"></p><!-- spacer -->

`dump` excludes secrets unless allowed.
```bash
pda dump
# nil

pda dump --secret
# {"key":"password","value":"hunter2","encoding":"text"}
```

<p align="center"></p><!-- spacer -->

### TTL

`ttl` sets an expiration time. Expired keys get marked for garbage collection and will be deleted on the next-run of the store. They wont be accessible.
```bash
# Expire after 1 hour
pda set session "123" --ttl 1h

# After 52 minutes and 10 seconds
pda set session2 "xyz" --ttl 54m10s
```

<p align="center"></p><!-- spacer -->

`list --ttl` shows expiration date in list output.
```bash
pda ls --ttl
# session    123    2025-11-21T15:30:00Z (in 59m30s)
# session2   xyz    2025-11-21T15:21:40Z (in 51m40s)
```

`dump` and `restore` persists the expiry date. Expirations will continue ticking down regardless of if they're actively in a store or not - the expiry is just a timestamp, not a timer.

<p align="center"></p><!-- spacer -->

### Binary

Save binary data.
```bash
pda set logo < logo.png```
```

<p align="center"></p><!-- spacer -->

And `get` it like normal.
```bash
pda get logo > output.png
```

<p align="center"></p><!-- spacer -->

`list` and `get` will omit binary data whenever it's a human reading it. If it's being piped somewhere or ran outside of a TTY, it'll output the whole data.

`include-binary` to show the full binary data regardless.
```bash
pda get logo
# (omitted binary data)

pda get logo --include-binary
# 89504E470D0A1A0A0000000D4948445200000001000000010802000000
```

<p align="center"></p><!-- spacer -->

`dump` encodes binary data as base64.
```bash
pda dump
# {"key":"logo","value":"89504E470D0A1A0A0000000D4948445200000001000000010802000000","encoding":"base64"}
```

<p align="center"></p><!-- spacer -->

### Environment

Data is stored in your user data directory under `pda/stores/`.

Usually:
- linux: `~/.local/share/pda/stores/`
- macOS: `~/Library/Application Support/pda/stores/`
- windows: `%LOCALAPPDATA%/pda/stores/`

`PDA_DATA_DIR` overrides the default storage location.
```bash
PDATA_DATA_DIR=/tmp/stores pda set key value
```

<p align="center"></p><!-- spacer -->

`pda get --run` uses `SHELL` for command execution.
```bash
# SHELL is usually your current shell.
pda get script --run

# An empty SHELL falls back to using 'sh'.
export SHELL=""
pda get script --run
```

<p align="center"></p><!-- spacer -->
