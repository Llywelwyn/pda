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
- [templates](https://github.com/Llywelwyn/pda#templates) supporting arbitrary shell execution, conditionals, loops, more,
- [encryption](https://github.com/Llywelwyn/pda#encryption) at rest using [age](https://github.com/FiloSottile/age),
- Git-backed [version control](https://github.com/Llywelwyn/pda#git) with automatic syncing,
- [search and filtering](https://github.com/Llywelwyn/pda#filtering) by key, value, or store,
- plaintext exports in 7 different formats,
- support for all [binary data](https://github.com/Llywelwyn/pda#binary),
- expiring keys with a [time-to-live](https://github.com/Llywelwyn/pda#ttl),
- built-in [diagnostics](https://github.com/Llywelwyn/pda#doctor) and [configuration](https://github.com/Llywelwyn/pda#config),

and more, written in pure Go, and inspired by [skate](https://github.com/charmbracelet/skate) and [nb](https://github.com/xwmx/nb).

<p align="center"></p><!-- spacer -->

`pda!` stores key-value pairs natively as [newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Newline-delimited_JSON) files. The `list` command outputs tabular data by default, but also supports [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Tab-separated_values), [Markdown](https://en.wikipedia.org/wiki/Markdown) and [HTML](https://en.wikipedia.org/wiki/HTML_element#Tables) tables, JSON, and raw NDJSON. Because every store is in plaintext, Git versioning is pretty easy: auto-committing, pushing, and fetching can be enabled in the config to automatically version changes, or just `pda sync` regularly.

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
- [Filtering](https://github.com/Llywelwyn/pda#filtering)
- [TTL](https://github.com/Llywelwyn/pda#ttl)
- [Binary](https://github.com/Llywelwyn/pda#binary)
- [Encryption](https://github.com/Llywelwyn/pda#encryption)
- [Doctor](https://github.com/Llywelwyn/pda#doctor)
- [Config](https://github.com/Llywelwyn/pda#config)
- [Environment](https://github.com/Llywelwyn/pda#environment)

<p align="center"></p><!-- spacer -->

### Overview

  ```bash
pda! MIT licensed. (c) 2025 Lewis Wynne

Usage:
  pda [command]

Key commands:
  copy         Make a copy of a key
  edit         Edit a key's value in $EDITOR
  get          Get the value of a key
  identity     Show or create the age encryption identity
  list         List the contents of all stores
  meta         View or modify metadata for a key
  move         Move a key
  remove       Delete one or more keys
  run          Get the value of a key and execute it
  set          Set a key to a given value

Store commands:
  export       Export store as NDJSON (alias for list --format ndjson)
  import       Restore key/value pairs from an NDJSON dump
  list-stores  List all stores
  move-store   Rename a store
  remove-store Delete a store

Git commands:
  git          Run any arbitrary command. Use with caution.
  init         Initialise pda! version control
  sync         Manually sync your stores with Git

Environment commands:
  config       View and modify configuration
  doctor       Check environment health

Additional Commands:
  completion   Generate the autocompletion script for the specified shell
  help         Help about any command
  version      Display pda! version
```

<p align="center"></p><!-- spacer -->

Most commands have aliases and flags. `pda help [command]` to see them.

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

# From a file
pda set dogs --file dogs.txt
pda set kitty -f cat.png

# --safe to skip if the key already exists.
pda set name "Alice" --safe
pda set name "Bob" --safe
pda get name
# Alice
```

<p align="center"></p><!-- spacer -->

`pda get` to retrieve it.
```bash
pda get name
# Alice

# Or run it directly.
pda run name
# same as: pda get name --run

# Check if a key exists (exit 0 if found, exit 1 if not).
pda get name --exists
```

<p align="center"></p><!-- spacer -->

`pda edit` to open a key in your `$EDITOR`.
```bash
# Edit an existing key.
pda edit name

# Edit a key that doesn't exist yet — saving non-empty content creates it.
pda edit newkey

# Edit and modify metadata in the same operation.
pda edit name --ttl 1h --encrypt

# Trailing newlines added by the editor are stripped by default.
# Pass --preserve-newline to keep them.
pda edit name --preserve-newline
```

<p align="center"></p><!-- spacer -->

`pda mv` to move it.
```bash
pda mv name name2
#   ok renamed name to name2

# --safe to skip if the destination already exists.
pda mv name name2 --safe
# info skipped 'name2': already exists

# --yes/-y to skip confirmation prompts.
pda mv name name2 -y
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

# Mix exact keys with glob patterns.
pda set cog "cogs"
pda set dog "doggy"
pda set kitty "cat"
pda rm kitty --key "?og"

# Opt in to a confirmation prompt with --interactive/-i (or always_prompt_delete in config).
pda rm kitty -i
#  ??? remove 'kitty'? (y/n)
#  ==> y

# --yes/-y to auto-accept all confirmation prompts.
pda rm kitty -y
```

<p align="center"></p><!-- spacer -->

`pda ls` to see what you've got stored. By default it lists the contents of all stores. Pass a store name to check only the given store. Checking a specific store is faster than checking everything, but the slowdown should be insignificant unless you have masses of different stores. `list.always_show_all_stores` can be set to false to list `store.default_store_name` by default.
```bash
pda ls
# Key  Store    Value                TTL
# dogs default  four legged mammals  no expiry
# name default  Alice                no expiry

# Narrow to a single store.
pda ls @default

# Or filter stores by glob pattern.
pda ls --store "prod*"

# Or as CSV.
pda ls --format csv
# Key,Store,Value,TTL
# dogs,default,four legged mammals,no expiry
# name,default,Alice,no expiry

# Or as a JSON array.
pda ls --format json
# [{"key":"dogs","value":"four legged mammals","encoding":"text","store":"default"},{"key":"name","value":"Alice","encoding":"text","store":"default"}]

# Or TSV, Markdown, HTML, NDJSON.

# Just the count of entries.
pda ls --count
# 2
pda ls --count --key "d*"
# 1
```

<p align="center"></p><!-- spacer -->

Long values are truncated to fit the terminal. Use `--full`/`-f` to show the complete value.
```bash
pda ls
# Key  Value                                 TTL
# note this is a very long (..30 more chars) no expiry

pda ls --full
# Key  Value                                                   TTL
# note this is a very long value that keeps on going and going no expiry
```

<p align="center"></p><!-- spacer -->

`pda export` to export everything as NDJSON.
```bash
pda export > my_backup

# Export only matching keys.
pda export --key "a*"

# Export only entries whose values contain a URL.
pda export --value "**https**"
```

<p align="center"></p><!-- spacer -->

`pda import` to import it all back. By default, each entry is routed to the store it came from (via the `"store"` field in the NDJSON). If no `"store"` field is present, entries go to the default store. Pass a store name as a positional argument to force all entries into one store. Existing keys are updated and new keys are added.
```bash
# Entries are routed to their original stores.
pda import -f my_backup
#   ok restored 5 entries

# Force all entries into a specific store by passing a store name.
pda import mystore -f my_backup
#   ok restored 5 entries into @mystore

# Or from stdin.
pda import < my_backup

# Import only matching keys.
pda import --key "a*" -f my_backup

# Import only entries from matching stores.
pda import --store "prod*" -f my_backup

# Full replace — drop all existing entries before importing.
pda import --drop -f my_backup
```

<p align="center"></p><!-- spacer -->

You can have as many stores as you want. All the store commands have shorthands, like `mv` to move a key, or `mvs` to move a store.
```bash
# Save to a specific store.
pda set alice@birthdays 11/11/1998

# See which stores have contents.
pda list-stores
# Keys  Size Store
#    2 1.8k @birthdays
#   12 4.2k @default

# Just the names.
pda list-stores --short
# @birthdays
# @default

# Check out a specific store.
pda ls @birthdays --no-header --no-ttl
# alice 11/11/1998
# bob   05/12/1980

# Export it.
pda export birthdays > friends_birthdays

# Import it.
pda import birthdays < friends_birthdays

# Rename it.
pda move-store birthdays bdays

# Or copy it.
pda move-store birthdays bdays --copy

# --safe to skip if the destination already exists.
pda move-store birthdays bdays --safe

# Delete it.
pda remove-store birthdays

# --yes/-y to skip confirmation prompts on delete or overwrite.
pda remove-store birthdays -y
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

# With a custom commit message.
pda sync -m "added production credentials"
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

To fit `text/template` nicely into this tool, pda has a sparse set of additional functions built-in. For example, `default` values, `enum`s, `require`d values, `time`, `lists`, arbitrary `shell` execution, and getting other `pda` keys (recursively!). These same functions are also available in `git.default_commit_message` templates, along with `summary` which returns the action that triggered the commit (e.g. "set foo", "removed bar").

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
# FAIL cannot get 'file': ...required value is missing or empty
```

<p align="center"></p><!-- spacer -->

`env` reads from environment variables.
```bash
pda set my_name "{{ env "USER" }}"
pda get my_name
# llywelwyn
```

<p align="center"></p><!-- spacer -->

`time` returns the current UTC time in RFC3339 format.
```bash
pda set note "Created at {{ time }}"
pda get note
# Created at 2025-01-15T12:00:00Z
```

<p align="center"></p><!-- spacer -->

`enum` restricts acceptable values.
```bash
pda set level "Log level: {{ enum .LEVEL "info" "warn" "error" }}"
pda get level LEVEL=info
# Log level: info
pda get level LEVEL=debug
# FAIL cannot get 'level': ...invalid value 'debug', allowed: [info warn error]
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

`shell` executes a command and returns stdout.
```bash
pda set rev '{{ shell "git rev-parse --short HEAD" }}'
pda get rev
# a1b2c3d

pda set today '{{ shell "date +%Y-%m-%d" }}'
pda get today
# 2025-06-15
```

<p align="center"></p><!-- spacer -->

`pda` gets another key.
```bash
pda set base_url "https://api.example.com"
pda set endpoint '{{ pda "base_url" }}/users/{{ require .ID }}'
pda get endpoint ID=42
# https://api.example.com/users/42

# Cross-store references work too.
pda set host@urls "https://example.com"
pda set api '{{ pda "host@urls" }}/api'
pda get api
# https://example.com/api
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

### Filtering

`--key`/`-k`, `--value`/`-v`, and `--store`/`-s` can be used as filters with glob support. `gobwas/glob` is used for matching. All three flags are repeatable, with results matching one-or-more of the patterns passed per flag. When multiple flags are combined, results must satisfy all of them (AND across flags, OR within the same flag).

`--key`, `--value`, and `--store` filters work with `list`, `export`, `import`, and `remove`. `--value` is not available on `import` or `remove`.

<p align="center"></p><!-- spacer -->

`*` wildcards a word or series of characters, stopping at separator boundaries (the default separators are `/-_.@:` and space).
```bash
pda ls --no-values --no-header
# cat
# dog
# cog
# mouse hotdog
# mouse house
# foo.bar.baz

pda ls --key "*"
# cat
# dog
# cog

pda ls --key "* *"
# mouse hotdog
# mouse house

pda ls --key "foo.*.baz"
# foo.bar.baz
```

<p align="center"></p><!-- spacer -->

`**` super-wildcards ignore word boundaries.
```bash
pda ls --key "foo**"
# foo.bar.baz

pda ls --key "**g"
# dog
# cog
# mouse hotdog
```

<p align="center"></p><!-- spacer -->

`?` wildcards a single letter.
```bash
pda ls --key "?og"
# dog
# cog
# frog --> fail
# dogs --> fail
```

<p align="center"></p><!-- spacer -->

`[abc]` must match one of the characters in the brackets.
```bash
pda ls --key "[dc]og"
# dog
# cog
# bog --> fail

# Can be negated with '!'
pda ls --key "[!dc]og"
# dog --> fail
# cog --> fail
# bog
```

<p align="center"></p><!-- spacer -->

`[a-c]` must fall within the range given in the brackets.
```bash
pda ls --key "[a-g]ag"
# bag
# gag
# wag --> fail

# Can be negated with '!'
pda ls --key "[!a-g]ag"
# bag --> fail
# gag --> fail
# wag

pda ls --key "19[90-99]"
# 1991
# 1992
# 2001 --> fail
# 1988 --> fail
```

<p align="center"></p><!-- spacer -->

`--value` filters by value content using the same glob syntax.
```bash
pda ls --value "**localhost**"
# db-url  postgres://localhost:5432  no expiry

# Combine key and value filters.
pda ls --key "db*" --value "**localhost**"
# db-url  postgres://localhost:5432  no expiry

# Multiple --value patterns are OR'd.
pda ls --value "**world**" --value "42"
# greeting  hello world  no expiry
# number    42           no expiry
```

<p align="center"></p><!-- spacer -->

Globs can be arbitrarily complex, and `--key` can be combined with exact positional args on `rm`.
```bash
pda rm cat --key "{mouse,[cd]og}**"
#  ??? remove 'cat'? (y/n)
#  ==> y
#  ??? remove 'mouse trap'? (y/n)
# ...
```

Locked (encrypted without an available identity) and non-UTF-8 (binary) entries are silently excluded from `--value` matching.

<p align="center"></p><!-- spacer -->

### TTL

`ttl` sets an expiration time. Expired keys get marked for garbage collection and will be deleted on the next-run of the store. They wont be accessible.
```bash
# Expire after 1 hour
pda set session "123" --ttl 1h

# After 54 minutes and 10 seconds
pda set session2 "xyz" --ttl 54m10s
```

<p align="center"></p><!-- spacer -->

`list` shows expiration in the TTL column by default.
```bash
pda ls
# Key       Value TTL
# session   123   in 59m30s
# session2  xyz   in 51m40s
```

`export` and `import` persist the expiry date. Expirations will continue ticking down regardless of if they're actively in a store or not - the expiry is just a timestamp, not a timer.

<p align="center"></p><!-- spacer -->

`pda meta` views or modifies metadata (TTL, encryption) without changing a key's value.
```bash
# View metadata for a key.
pda meta session
#   key: session@default
#   secret: false
#   expires: in 59m30s

# Set or change TTL.
pda meta session --ttl 2h

# Clear TTL.
pda meta session --ttl never

# Encrypt an existing plaintext key.
pda meta api-key --encrypt

# Decrypt an encrypted key.
pda meta api-key --decrypt
```

<p align="center"></p><!-- spacer -->

### Binary

Save binary data.
```bash
pda set logo < logo.png
```

<p align="center"></p><!-- spacer -->

And `get` it like normal.
```bash
pda get logo > output.png
```

<p align="center"></p><!-- spacer -->

`list` and `get` will show a summary for binary data on a TTY. If it's being piped somewhere or ran outside of a TTY, it'll output the raw bytes.

`--base64`/`-b` to view binary data as base64 on a TTY.
```bash
pda get logo
# (binary: 4.2 KB, image/png)

pda get logo --base64
# iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADklEQVQI12...
```

<p align="center"></p><!-- spacer -->

`export` encodes binary data as base64.
```bash
pda export
# {"key":"logo","value":"89504E470D0A1A0A0000000D4948445200000001000000010802000000","encoding":"base64"}
```

<p align="center"></p><!-- spacer -->

### Encryption

`pda set --encrypt` encrypts values at rest using [age](https://github.com/FiloSottile/age). Values are stored on disk as age ciphertext and decrypted automatically by commands like `get` and `list` when the correct identity file is present. An X25519 identity is generated on first use and saved at `~/.config/pda/identity.txt`.

```bash
pda set --encrypt api-key "sk-live-abc123"
#   ok created identity at ~/.config/pda/identity.txt

pda set --encrypt token "ghp_xxxx"
```

<p align="center"></p><!-- spacer -->

`get` decrypts automatically.
```bash
pda get api-key
# sk-live-abc123
```

<p align="center"></p><!-- spacer -->

The on-disk value is ciphertext, so encrypted entries are safe to commit and push with Git.
```bash
pda export
# {"key":"api-key","value":"YWdlLWVuY3J5cHRpb24u...","encoding":"secret"}
```

<p align="center"></p><!-- spacer -->

`mv`, `cp`, and `import` all preserve encryption. Overwriting an encrypted key without `--encrypt` will warn you.
```bash
pda cp api-key api-key-backup
# still encrypted

pda set api-key "oops"
# WARN overwriting encrypted key 'api-key' as plaintext
# hint pass --encrypt to keep it encrypted
```

<p align="center"></p><!-- spacer -->

If the identity file is missing, encrypted values are inaccessible but not lost. Keys are still visible, and the ciphertext is preserved through reads and writes.
```bash
pda ls
# Key     Value                          TTL
# api-key locked (identity file missing) no expiry

pda get api-key
# FAIL cannot get 'api-key': secret is locked (identity file missing)
```

<p align="center"></p><!-- spacer -->

`pda identity` to see your public key and identity file path.
```bash
pda identity
#   ok pubkey  age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
#   ok identity  ~/.config/pda/identity.txt

# Just the path.
pda identity --path
# ~/.config/pda/identity.txt

# Generate a new identity. Errors if one already exists.
pda identity --new
```

<p align="center"></p><!-- spacer -->

By default, secrets are encrypted only for your own identity. To encrypt for additional recipients (e.g. a teammate or another device), use `--add-recipient` with their age public key. All existing secrets are automatically re-encrypted for every recipient.
```bash
# Add a recipient. All secrets are re-encrypted for both keys.
pda identity --add-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p
#   ok re-encrypted api-key
#   ok added recipient age1ql3z...
#   ok re-encrypted 1 secret(s)

# Remove a recipient. Secrets are re-encrypted without their key.
pda identity --remove-recipient age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p

# Additional recipients are shown in the default identity display.
pda identity
#   ok pubkey     age1abc...
#   ok identity   ~/.local/share/pda/identity.txt
#   ok recipient  age1ql3z...
```

<p align="center"></p><!-- spacer -->

### Doctor

`pda doctor` runs a set of health checks of your environment.

```bash
pda doctor
#   ok pda! 2025.52 Christmas release (linux/amd64)
#   ok OS: Linux 6.18.7-arch1-1
#   ok Go: go1.23.0
#   ok Git: 2.45.0
#   ok Shell: /bin/zsh
#   ok Config: /home/user/.config/pda
#   ok Non-default config:
#      ├── display_ascii_art: false
#      └── git.auto_commit: true
#   ok Data: /home/user/.local/share/pda
#   ok Identity: /home/user/.config/pda/identity.txt
#   ok Git initialised on main
#   ok Git remote configured
#   ok Git in sync with remote
#   ok 3 store(s), 15 key(s), 2 secret(s), 4.2k total size
#   ok No issues found
```

<p align="center"></p><!-- spacer -->

Severity levels are colour-coded: `ok` (green), `WARN` (yellow), and `FAIL` (red). Only `FAIL` produces a non-zero exit code. `WARN` is generally not a problem, but may mean some functionality isn't being made use of, like for example version control not having been initialised yet.

<p align="center"></p><!-- spacer -->

### Config

Config is stored at `~/.config/pda/config.toml` (Linux/macOS) or `%LOCALAPPDATA%/pda/config.toml` (Windows). All values have sensible defaults, so a config file is entirely optional.

<p align="center"></p><!-- spacer -->

`pda config` manages configuration without editing files by hand.
```bash
# List all config values and their current settings.
pda config list

# Get a single value.
pda config get git.auto_commit
# false

# Set a value. Validated before saving.
pda config set git.auto_commit true

# Open in $EDITOR. Validated on save.
pda config edit

# Print the config file path.
pda config path

# Generate a fresh default config file.
pda config init

# Overwrite an existing config with defaults.
pda config init --new

# Update config: migrate deprecated keys and fill missing defaults.
pda config init --update
```

<p align="center"></p><!-- spacer -->

`pda doctor` will warn about unrecognised keys (typos, removed options) and show any non-default values, so it doubles as a config audit.

<p align="center"></p><!-- spacer -->

#### Example config.toml

All values below are the defaults. A missing config file or missing keys will use these values.

```toml
# display ascii header in long root and version commands
display_ascii_art = true 

[key]
# prompt y/n before deleting keys
always_prompt_delete = false
# prompt y/n before deleting with a glob match
always_prompt_glob_delete = true
# prompt y/n before key overwrites
always_prompt_overwrite = false
# encrypt all values at rest by default
always_encrypt = false

[store]
# store name used when none is specified
default_store_name = "default"
# prompt y/n before deleting whole store
always_prompt_delete = true
# prompt y/n before store overwrites
always_prompt_overwrite = true

[list]
# list all, or list only the default store when none specified
always_show_all_stores = true
# default output, accepts: table|tsv|csv|markdown|html|ndjson|json
default_list_format = "table"
# show full values without truncation
always_show_full_values = false
# suppress the header row
always_hide_header = false
# columns and order, accepts: key,store,value,ttl
default_columns = "key,store,value,ttl"

[git]
# auto fetch whenever a change happens
auto_fetch = false
# auto commit any changes
auto_commit = false
# auto push after committing
auto_push = false
# commit message if none manually specified
# supports templates, see: #templates section
default_commit_message = "{{ summary }} {{ time }}"
```

<p align="center"></p><!-- spacer -->

### Environment

`PDA_CONFIG` overrides the config directory. pda! will look for `config.toml` in this directory.
```bash
PDA_CONFIG=/tmp/config/ pda set key value
```

<p align="center"></p><!-- spacer -->

`PDA_DATA` overrides the data storage directory.

Default locations:
- Linux: `~/.local/share/pda/`
- macOS: `~/Library/Application Support/pda/`
- Windows: `%LOCALAPPDATA%/pda/`

```bash
PDA_DATA=/tmp/stores pda set key value
```

<p align="center"></p><!-- spacer -->

`EDITOR` is used by `pda edit` and `pda config edit` to open values in a text editor. Must be set for these commands to work.
```bash
EDITOR=nvim pda edit mykey
```

<p align="center"></p><!-- spacer -->

`SHELL` is used by `pda run` (or `pda get --run`) for command execution. Falls back to `/bin/sh` if unset.
```bash
pda run script
```

<p align="center"></p><!-- spacer -->

### Version

`pda!` uses calendar versioning: `YYYY.WW`. ASCII art can be permanently disabled with `display_ascii_art = false` in config.

```bash
# Display the full version output.
pda version

# Or just the release.
pda version --short
# pda! 2025.47 release
```

<p align="center"></p><!-- spacer -->
