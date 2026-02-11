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

`pda!` is a command-line key-value store tool with:
- [templates](https://github.com/Llywelwyn/pda#templates),
- search and filtering with [globs](https://github.com/Llywelwyn/pda#globs),
- Git-backed [version control](https://github.com/Llywelwyn/pda#git),
- plaintext exports in multiple formats,
- support for [binary data](https://github.com/Llywelwyn/pda#binary),
- [time-to-live](https://github.com/Llywelwyn/pda#ttl) support,

and more, written in pure Go, and inspired by [skate](https://github.com/charmbracelet/skate) and [nb](https://github.com/xwmx/nb).

<p align="center"></p><!-- spacer -->

`pda!` stores key-value pairs natively as [newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Newline-delimited_JSON) files. The `list` command outputs tabular data by default, but also supports [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Tab-separated_values), [Markdown](https://en.wikipedia.org/wiki/Markdown) and [HTML](https://en.wikipedia.org/wiki/HTML_element#Tables) tables, and raw NDJSON. Because every store is in plaintext, Git versioning is pretty easy: auto-committing, pushing, and fetching can be enabled in the config to automatically version changes, or just `pda sync` regularly.

<p align="center"></p><!-- spacer -->

<div align="center">
  <a href="https://github.com/llywelwyn/pda/actions" rel="nofollow">
    <img src="https://github.com/Llywelwyn/pda/blob/main/vhs/intro.gif"
         alt="build status"
         style="max-width:50%;">
  </a>
</div>

<p align="center"></p><!-- spacer -->

### Contents

- [Overview](https://github.com/Llywelwyn/pda#overview)
- [Installation](https://github.com/Llywelwyn/pda#installation)
- [Get Started](https://github.com/Llywelwyn/pda#get-started)
- [Git-backed version control](https://github.com/Llywelwyn/pda#git)
- [Templates](https://github.com/Llywelwyn/pda#templates)
- [Globs](https://github.com/Llywelwyn/pda#globs)
- [TTL](https://github.com/Llywelwyn/pda#ttl)
- [Binary](https://github.com/Llywelwyn/pda#binary)
- [Environment](https://github.com/Llywelwyn/pda#environment)

<p align="center"></p><!-- spacer -->

### Overview

  ```bash
                  ▄▄
                  ██
██▄███▄    ▄███▄██   ▄█████▄
██▀  ▀██  ██▀  ▀██   ▀ ▄▄▄██
██    ██  ██    ██  ▄██▀▀▀██
███▄▄██▀  ▀██▄▄███  ██▄▄▄███
██ ▀▀▀      ▀▀▀ ▀▀   ▀▀▀▀ ▀▀
██      (c) 2025 Lewis Wynne

Usage:
  pda [command]

Key commands:
  copy         Make a copy of a key
  get          Get the value of a key
  list         List the contents of a store
  move         Move a key
  remove       Delete one or more keys
  run          Get the value of a key and execute it
  set          Set a key to a given value

Store commands:
  export       Export store as NDJSON (alias for list --format ndjson)
  import       Restore key/value pairs from an NDJSON dump
  list-stores  List all stores
  remove-store Delete a store

Git commands:
  git          Run any arbitrary command. Use with caution.
  init         Initialise pda! version control
  sync         Manually sync your stores with Git

Additional Commands:
  completion   Generate the autocompletion script for the specified shell
  help         Help about any command
  version      Display pda! version
```

<p align="center"></p><!-- spacer -->

### Installation

```bash
# Get the latest release from the AUR
yay -S pda

# Or use pda-git for the latest commit
yay -S pda-git

# Go install
go install github.com/llywelwyn/pda@latest

# Or
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
pda run name
# same as: pda get name --run
```

<p align="center"></p><!-- spacer -->

`pda mv` to move it.
```bash
pda mv name name2
# renamed name to name2
```

`pda cp` to make a copy.
```bash
pda cp name name2

# 'mv --copy' and 'cp' are aliases. Either one works.
pda mv name name2 --copy
```

<p align="center"></p><!-- spacer -->

`pda rm` to delete one or more keys.
```bash
pda rm kitty

# Remove multiple keys, within the same or different stores.
pda rm kitty dog@animals

# Mix exact keys with globs.
pda set cog "cogs"
pda set dog "doggy"
pda set kitty "cat"
pda rm kitty --glob ?og
# Default glob separators: "/-_.@: " (space included). Override with --glob-sep.

# Opt in to a confirmation prompt with --interactive/-i (or always_prompt_delete in config).
pda rm kitty -i
# remove "kitty": are you sure? (y/n)
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

`pda export` to export everything as NDJSON.
```bash
pda export > my_backup

# Export only matching keys.
pda export --glob a*
```

<p align="center"></p><!-- spacer -->

`pda import` to import it all back.
```bash
# Import with an argument.
pda import -f my_backup
# Restored 2 entries into @default.

# Or from stdin.
pda import < my_backup
# Restored 2 entries into @default.

# Import only matching keys.
pda import --glob a* -f my_backup
```

<p align="center"></p><!-- spacer -->

You can have as many stores as you want.
```bash
# Save to a specific store.
pda set alice@birthdays 11/11/1998

# See which stores have contents.
pda list-stores
# @default
# @birthdays

# Check out a specific store.
pda ls @birthdays
# alice    11/11/1998
# bob      05/12/1980

# Export it.
pda export birthdays > friends_birthdays

# Import it.
pda import birthdays < friends_birthdays

# Delete it.
pda rm-store birthdays
```

<p align="center"></p><!-- spacer -->

### Git

pda! supports automatic version control backed by Git, either in a local-only repository or by initialising from a remote repository.

`pda init` will initialise the version control system.
```bash
# Initialise an empty pda! repository.
pda init

# Or clone an existing one.
pda init https://github.com/llywelwyn/my-repository

# --clean to replace your (existing) local repo with a new one.
pda init --clean
```

<p align="center"></p><!-- spacer -->

`pda sync` conducts a best-effort syncing of your local data with your Git repository. Any time you swap machine or know you've made changes outside of `pda!` itself, I recommend syncing.

If you're ahead of your Git repo, syncing will add your changes, commit them, and push to remote if a remote is set. If you use multiple devices or otherwise end up behind your Git repo, syncing will detect this and give you a prompt: either stash your local changes and pull the latest commit from version control, or abort and fix the issue manually.

```bash
# Sync with Git
pda sync
```

`pda!` supports some automation via its config. There are options for `git.auto_commit`, `git.auto_fetch`, and `git.auto_push`. Any of these operations will slow down `pda!` because it means versioning with every change, but it does effectively guarantee never managing to desync oneself and requiring manual fixes, and reduces the frequency with which one will need to manually run the sync command.

Auto-commit will commit changes immediately to the local Git repository any time `pda!` data is changed. Auto-fetch will fetch before committing any changes, but incurs a significant slowdown in operations simply due to the time a fetch takes. Auto-push will automatically push committed changes to the remote repository, if one is set.

If auto-commit is set to false, auto-fetch and auto-push will do nothing. They can be considered to be additional steps taken during the commit process.

Running `pda sync` manually will always fetch, commit, and push - or if behind it will fetch, stash, and pull - regardless of config.

My general recommendation would be to enable `git.auto_commit`, and to run a manual `pda sync` any time you're preparing to switch machines, or loading up a new one.

<p align="center"></p><!-- spacer -->

### Templates

Values support effectively all of Go's `text/template` syntax. Templates are evaluated on `pda get`.

`text/template` is a Turing-complete templating library that supports most of what you'd expect in a scripting language. Actions are given with ``{{ action }}`` syntax and support pipelines and nested templates, along with a lot more. I recommend reading the documentation if you want to do anything more complicated than described here.

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
pda set hello "{{ if .MORNING }}Good morning.{{ end }}"
pda get hello MORNING=1
# Good morning.
pda get hello --no-template
# {{ if .MORNING }}Good morning.{{ end }}
```

<p align="center"></p><!-- spacer -->

### Globs

Globs can be used in a few commands where their use makes sense. `gobwas/glob` is used for matching.

Searching for globs is inherently slower than looking for direct matches, so globs are opt-in via a repeatable `--glob/-g` flag by default rather than having every string treated as a glob by default. Realistically the performance impact will be negligible unless you have many thousands of entries in the same store.

<p align="center"></p><!-- spacer -->

`*` wildcards a word or series of characters.
```bash
pda ls --no-values
# cat
# dog
# cog
# mouse hotdog
# mouse house
# foo.bar.baz

pda ls --glob "*"
# cat
# dog
# cog

pda ls --glob "* *"
# mouse hotdog
# mouse house

pda ls --glob "foo.*.baz"
# foo.bar.baz
```

<p align="center"></p><!-- spacer -->

`**` super-wildcards ignore word boundaries.
```bash
pda ls --glob "foo**"
# foo.bar.baz

pda ls --glob "**g"
# dog
# cog
# mouse hotdog
```

<p align="center"></p><!-- spacer -->

`?` wildcards a single letter.
```bash
pda ls --glob ?og
# dog
# cog
# frog --> fail
# dogs --> fail
```

<p align="center"></p><!-- spacer -->

`[abc]` must match one of the characters in the brackets.
```bash
pda ls --glob [dc]og
# dog
# cog
# bog --> fail

# Can be negated with '!'
pda ls --glob [!dc]og
# dog --> fail
# cog --> fail
# bog
```

<p align="center"></p><!-- spacer -->

`[a-c]` must fall within the range given in the brackets
```bash
pda ls --glob [a-g]ag
# bag
# gag
# wag --> fail

# Can be negated with '!'
pda ls --glob [!a-g]ag
# bag --> fail
# gag --> fail
# wag

pda ls --glob 19[90-99]
# 1991
# 1992
# 2001 --> fail
# 1988 --> fail
```

<p align="center"></p><!-- spacer -->

Globs can be arbitrarily complex, and can be combined with strict matches.
```bash
pda ls --no-keys
# cat
# mouse trap
# dog house
# cat flap
# cogwheel

pda rm cat --glob "{mouse,[cd]og}**"
# remove: 'cat', 'mouse trap', 'dog house', 'cogwheel': are you sure? [y/n]
```

<p align="center"></p><!-- spacer -->

`--glob-sep` can be used to change the default list of separators used to determine word boundaries. Separators default to a somewhat reasonable list of common alphanumeric characters so should be usable in most usual situations.
```bash
pda ls --no-keys
# foo%baz

pda ls --glob "*"
# foo%baz

pda ls --glob "*" --glob-sep "%"
# foo%baz --> fail
# % is considered a word boundary, so "*" no longer matches.

pda ls --glob "*%*" --glob-sep "%"
# foo%baz
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

`export` and `import` persist the expiry date. Expirations will continue ticking down regardless of if they're actively in a store or not - the expiry is just a timestamp, not a timer.

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

`export` encodes binary data as base64.
```bash
pda export
# {"key":"logo","value":"89504E470D0A1A0A0000000D4948445200000001000000010802000000","encoding":"base64"}
```

<p align="center"></p><!-- spacer -->

### Environment

Config is stored in your user config directory in `pda/config.toml`.

Usually: `~/.config/pda/config.toml`

```
# ~/.config/pda/config.toml
display_ascii_art = true

[key]
always_prompt_delete = false
always_prompt_overwrite = false

[store]
default_store_name = "default"
always_prompt_delete = true

[git]
auto_fetch = false
auto_commit = true
auto_push = false
```

`PDA_CONFIG` overrides the default config location. pda! will look for a config.toml file in that directory.
```bash
PDA_CONFIG=/tmp/config/ pda set key value
```

<p align="center"></p><!-- spacer -->

Data is stored in your user data directory under `pda/stores/`.

Usually:
- linux: `~/.local/share/pda/stores/`
- macOS: `~/Library/Application Support/pda/stores/`
- windows: `%LOCALAPPDATA%/pda/stores/`

`PDA_DATA` overrides the default storage location.
```bash
PDA_DATA=/tmp/stores pda set key value
```

<p align="center"></p><!-- spacer -->

`pda run` (or `pda get --run`) uses `SHELL` for command execution.
```bash
# SHELL is usually your current shell.
pda run script

# An empty SHELL falls back to using 'sh'.
export SHELL=""
pda run script
```

<p align="center"></p><!-- spacer -->
